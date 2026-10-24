package dispatcher

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	panel "github.com/YeJianbo/ravel/api/v2board"
	"github.com/YeJianbo/ravel/common/counter"
	"github.com/YeJianbo/ravel/limiter"
	proxymanoutbound "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
)

func accountingTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	l, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	a, err := net.DialTCP("tcp4", nil, l.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	b, err := l.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	deadline := time.Now().Add(10 * time.Second)
	_ = a.SetDeadline(deadline)
	_ = b.SetDeadline(deadline)
	return a, b
}

func TestVisionRawCopyAccountsManagedTraffic(t *testing.T) {
	payload := bytes.Repeat([]byte("accounting-test!"), 65536)
	for _, name := range []string{"upload", "download"} {
		t.Run(name, func(t *testing.T) {
			limiter.Init()
			limiter.AddLimiter("vless", "vision", []panel.UserInfo{{Id: 7, Uuid: "user"}}, nil)
			t.Cleanup(func() { limiter.DeleteLimiter("vision") })
			d := &DefaultDispatcher{}
			t.Cleanup(func() { _ = d.Close() })
			sender, reader := accountingTCPPair(t)
			writer, receiver := accountingTCPPair(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			inbound := &session.Inbound{
				Tag: "vision", User: &protocol.MemoryUser{Email: "vision|user"},
				Source: xnet.TCPDestination(xnet.LocalHostIP, 1234), CanSpliceCopy: 1,
			}
			ctx = session.ContextWithInbound(ctx, inbound)
			ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{CanSpliceCopy: 1}})
			timer := signal.CancelAfterInactivity(ctx, cancel, time.Minute)
			defer timer.SetTimeout(0)
			in, out, _, err := d.getLink(ctx)
			if err != nil {
				t.Fatal(err)
			}
			value, _ := d.Counter.Load("vision")
			counts := value.(*counter.TrafficCounter).GetCounter("vision|user")
			managed := in.Writer.(*ManagedWriter)
			counted := &counts.UpCounter
			if name == "download" {
				managed = out.Writer.(*ManagedWriter)
				counted = &counts.DownCounter
			}
			// Keep the actual dispatcher wrappers, replacing only their pipe sink with TCP.
			_ = common.Close(managed.writer)
			managed.writer = buf.NewWriter(writer)
			sent := make(chan error, 1)
			go func() {
				_, err := io.Copy(sender, bytes.NewReader(payload))
				_ = sender.CloseWrite()
				sent <- err
			}()
			copied := make(chan error, 1)
			go func() {
				err := proxy.CopyRawConnIfExist(ctx, reader, writer, managed, timer, nil)
				_ = writer.CloseWrite()
				copied <- err
			}()
			got, err := io.ReadAll(receiver)
			if err != nil {
				t.Fatal(err)
			}
			if err := <-sent; err != nil {
				t.Fatal(err)
			}
			if err := <-copied; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("received %d bytes, want %d identical bytes", len(got), len(payload))
			}
			t.Logf("received=%d user_counter=%d", len(got), counted.Load())
			want := int64(len(payload))
			if counted.Load() != want {
				t.Fatalf("lost user traffic: got %d, want %d", counted.Load(), want)
			}
		})
	}
}

func TestDispatchLinkDisablesRawSpliceWithoutThrottle(t *testing.T) {
	limiter.Init()
	limiter.AddLimiter("vless", "vision-link", []panel.UserInfo{{Id: 7, Uuid: "user"}}, nil)
	t.Cleanup(func() { limiter.DeleteLimiter("vision-link") })
	d := &DefaultDispatcher{ohm: &proxymanoutbound.Manager{}}
	t.Cleanup(func() { _ = d.Close() })
	inbound := &session.Inbound{
		Tag: "vision-link", User: &protocol.MemoryUser{Email: "vision-link|user"},
		Source: xnet.TCPDestination(xnet.LocalHostIP, 1234), CanSpliceCopy: 2,
	}
	ctx := session.ContextWithInbound(context.Background(), inbound)
	link := &transport.Link{Reader: buf.NewReader(bytes.NewReader(nil)), Writer: buf.NewWriter(io.Discard)}
	if err := d.DispatchLink(ctx, xnet.TCPDestination(xnet.LocalHostIP, 443), link); err != nil {
		t.Fatal(err)
	}
	if inbound.CanSpliceCopy != 3 {
		t.Fatal("unthrottled authenticated link can bypass user accounting")
	}
}
