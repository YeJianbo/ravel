package dispatcher

import (
	"io"
	"testing"
	"time"

	"github.com/YeJianbo/ravel/common/domaintraffic"
	"github.com/xtls/xray-core/common/buf"
)

type heldDomainIO struct{ entered, release chan struct{} }

func (h *heldDomainIO) ReadMultiBuffer() (buf.MultiBuffer, error) {
	close(h.entered)
	<-h.release
	b := buf.New()
	_, _ = b.Write([]byte("tail"))
	return buf.MultiBuffer{b}, io.EOF
}
func (h *heldDomainIO) ReadMultiBufferTimeout(time.Duration) (buf.MultiBuffer, error) {
	return h.ReadMultiBuffer()
}
func (h *heldDomainIO) WriteMultiBuffer(b buf.MultiBuffer) error {
	close(h.entered)
	<-h.release
	buf.ReleaseMulti(b)
	return nil
}

func TestDomainAccountingFinishesBeforeClose(t *testing.T) {
	for _, mode := range []string{"read", "timeout", "write"} {
		t.Run(mode, func(t *testing.T) {
			tag := t.Name()
			domaintraffic.Enable(tag, true)
			t.Cleanup(func() { domaintraffic.Remove(tag) })
			counter, release := domaintraffic.Acquire(tag, "tail.example")
			defer release()
			m := &LinkManager{links: make(map[*ManagedWriter]buf.Reader)}
			h := &heldDomainIO{make(chan struct{}), make(chan struct{})}
			r := &domainReader{Reader: h, counter: counter, manager: m}
			w := &domainWriter{Writer: h, counter: counter, manager: m}
			done := make(chan struct{})
			go func() {
				defer close(done)
				if mode == "write" {
					b := buf.New()
					_, _ = b.Write([]byte("tail"))
					_ = w.WriteMultiBuffer(buf.MultiBuffer{b})
				} else {
					var b buf.MultiBuffer
					if mode == "timeout" {
						b, _ = r.ReadMultiBufferTimeout(time.Second)
					} else {
						b, _ = r.ReadMultiBuffer()
					}
					buf.ReleaseMulti(b)
				}
			}()
			<-h.entered
			m.mu.Lock()
			m.closed = true
			m.mu.Unlock()
			closed := make(chan struct{})
			go func() { m.CloseAll(); close(closed) }()
			select {
			case <-closed:
				t.Fatal("close skipped domain accounting")
			case <-time.After(10 * time.Millisecond):
			}
			close(h.release)
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("close did not finish")
			}
			records := domaintraffic.Drain(tag)
			if len(records) != 1 || records[0].Upload+records[0].Download != 4 {
				t.Fatalf("final counters: %+v", records)
			}
			<-done
			if _, err := r.ReadMultiBuffer(); err != io.ErrClosedPipe {
				t.Fatalf("late read: %v", err)
			}
			if _, err := r.ReadMultiBufferTimeout(time.Second); err != io.ErrClosedPipe {
				t.Fatalf("late timeout read: %v", err)
			}
			if err := w.WriteMultiBuffer(nil); err != io.ErrClosedPipe {
				t.Fatalf("late write: %v", err)
			}
		})
	}
}
