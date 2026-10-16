package dispatcher

import (
	"github.com/YeJianbo/ravel/common/domaintraffic"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"io"
	"time"
)

type domainReader struct {
	buf.Reader
	counter *domaintraffic.Counter
	manager *LinkManager
}

func (r *domainReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if r.manager != nil {
		if !r.manager.beginIO(nil) {
			return nil, io.ErrClosedPipe
		}
		defer r.manager.active.Done()
	}
	b, err := r.Reader.ReadMultiBuffer()
	r.counter.Add(int64(b.Len()), 0)
	return b, err
}
func (r *domainReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	if r.manager != nil {
		if !r.manager.beginIO(nil) {
			return nil, io.ErrClosedPipe
		}
		defer r.manager.active.Done()
	}
	reader, ok := r.Reader.(buf.TimeoutReader)
	if !ok {
		return nil, buf.ErrNotTimeoutReader
	}
	b, err := reader.ReadMultiBufferTimeout(timeout)
	r.counter.Add(int64(b.Len()), 0)
	return b, err
}
func (r *domainReader) Interrupt()   { common.Interrupt(r.Reader) }
func (r *domainReader) Close() error { return common.Close(r.Reader) }

type domainWriter struct {
	buf.Writer
	counter *domaintraffic.Counter
	manager *LinkManager
}

func (w *domainWriter) WriteMultiBuffer(b buf.MultiBuffer) error {
	if w.manager != nil {
		if !w.manager.beginIO(nil) {
			buf.ReleaseMulti(b)
			return io.ErrClosedPipe
		}
		defer w.manager.active.Done()
	}
	size := int64(b.Len())
	err := w.Writer.WriteMultiBuffer(b)
	if err == nil {
		w.counter.Add(0, size)
	}
	return err
}
func (w *domainWriter) Close() error { return common.Close(w.Writer) }
func (w *domainWriter) Interrupt()   { common.Interrupt(w.Writer) }
