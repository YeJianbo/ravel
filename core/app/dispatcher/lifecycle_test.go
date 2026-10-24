package dispatcher

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	panel "github.com/YeJianbo/ravel/api/v2board"
	"github.com/YeJianbo/ravel/common/counter"
	"github.com/YeJianbo/ravel/limiter"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
)

type heldReader struct {
	entered, interrupted, release chan struct{}
	once                          sync.Once
}

func (r *heldReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	close(r.entered)
	<-r.release
	b := buf.New()
	_, _ = b.Write([]byte("tail"))
	return buf.MultiBuffer{b}, io.EOF
}
func (r *heldReader) Interrupt() { r.once.Do(func() { close(r.interrupted) }) }

type discardWriter struct{}

func (*discardWriter) WriteMultiBuffer(mb buf.MultiBuffer) error { buf.ReleaseMulti(mb); return nil }
func (*discardWriter) Close() error                              { return nil }

func TestCloseWaitsForFinalReadCounter(t *testing.T) {
	m := &LinkManager{links: make(map[*ManagedWriter]buf.Reader)}
	raw := &heldReader{entered: make(chan struct{}), interrupted: make(chan struct{}), release: make(chan struct{})}
	var count atomic.Int64
	r := &managedReader{reader: raw, manager: m, counter: &count}
	w := &ManagedWriter{writer: &discardWriter{}, manager: m, counter: new(atomic.Int64)}
	m.AddLink(w, raw)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		mb, _ := r.ReadMultiBuffer()
		buf.ReleaseMulti(mb)
	}()
	<-raw.entered
	// Normal writer cleanup must not hide a still-running read from shutdown.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { m.CloseAll(); close(closed) }()
	<-raw.interrupted
	select {
	case <-closed:
		t.Fatal("close returned before the final read")
	default:
	}
	close(raw.release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not finish after read exit")
	}
	<-readDone
	if count.Load() != 4 {
		t.Fatalf("final count=%d", count.Load())
	}
	if _, err := r.ReadMultiBuffer(); err != io.ErrClosedPipe {
		t.Fatalf("late read: %v", err)
	}
	b := buf.New()
	if err := w.WriteMultiBuffer(buf.MultiBuffer{b}); err != io.ErrClosedPipe {
		t.Fatalf("late write: %v", err)
	}
}

func TestCloseNodeWaitsWithoutStoppingOtherNodesAndCanReopen(t *testing.T) {
	d := &DefaultDispatcher{}
	t.Cleanup(func() { _ = d.Close() })
	a := d.linkManager("a", "a|user")
	b := d.linkManager("b", "b|user")
	raw := &heldReader{entered: make(chan struct{}), interrupted: make(chan struct{}), release: make(chan struct{})}
	var count atomic.Int64
	r := &managedReader{reader: raw, manager: a, counter: &count}
	w := &ManagedWriter{writer: &discardWriter{}, manager: a, counter: new(atomic.Int64)}
	a.AddLink(w, raw)
	go func() { mb, _ := r.ReadMultiBuffer(); buf.ReleaseMulti(mb) }()
	<-raw.entered
	closed := make(chan struct{})
	go func() { d.CloseNode("a"); close(closed) }()
	<-raw.interrupted
	if d.linkManager("a", "a|late") != nil {
		t.Fatal("closing node accepted a new manager")
	}
	if err := d.OpenNode("a"); err == nil {
		t.Fatal("reopened before old I/O finished")
	}
	if d.linkManager("b", "b|user") != b || !b.beginIO(nil) {
		t.Fatal("other node was stopped")
	}
	b.active.Done()
	close(raw.release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("node close did not finish")
	}
	if count.Load() != 4 {
		t.Fatalf("final node count=%d", count.Load())
	}
	if err := d.OpenNode("a"); err != nil {
		t.Fatal(err)
	}
	if next := d.linkManager("a", "a|user"); next == nil || next == a {
		t.Fatal("reopened node reused closed manager")
	}
}

func TestConcurrentFirstLinksKeepAllCountersAndClose(t *testing.T) {
	limiter.Init()
	limiter.AddLimiter("vmess", "node", []panel.UserInfo{{Id: 7, Uuid: "user"}}, map[int]int{})
	t.Cleanup(func() { limiter.DeleteLimiter("node") })
	d := &DefaultDispatcher{}
	t.Cleanup(func() { _ = d.Close() })
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
		Tag: "node", User: &protocol.MemoryUser{Email: "node|user"},
		Source: net.TCPDestination(net.LocalHostIP, 1234),
	})
	const connections = 32
	start := make(chan struct{})
	failures := make(chan error, connections)
	var workers sync.WaitGroup
	for i := 0; i < connections; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			inbound := *session.InboundFromContext(ctx)
			in, out, _, err := d.getLink(session.ContextWithInbound(ctx, &inbound))
			if err != nil {
				failures <- err
				return
			}
			for _, writer := range []buf.Writer{in.Writer, out.Writer} {
				b := buf.New()
				_, _ = b.Write([]byte("traffic"))
				if err := writer.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	close(start)
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	value, ok := d.Counter.Load("node")
	if !ok {
		t.Fatal("node counter missing")
	}
	c := value.(*counter.TrafficCounter)
	if c.GetUpCount("node|user") != connections*7 || c.GetDownCount("node|user") != connections*7 {
		t.Fatalf("lost traffic: up=%d down=%d", c.GetUpCount("node|user"), c.GetDownCount("node|user"))
	}
	m := d.linkManager("node", "node|user")
	if len(m.links) != connections*2 {
		t.Fatalf("tracked links=%d", len(m.links))
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if len(m.links) != 0 {
		t.Fatal("links retained after close")
	}
	if _, _, _, err := d.getLink(ctx); err == nil {
		t.Fatal("closed dispatcher accepted a link")
	}
}
