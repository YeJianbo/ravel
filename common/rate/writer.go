package rate

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/ratelimit"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

type Writer struct {
	writer    buf.Writer
	limiter   *DynamicBucket
	done      chan struct{}
	closeOnce sync.Once
}

type DynamicBucket struct {
	v atomic.Value // *ratelimit.Bucket
}

func NewDynamicBucket(rate int64) *DynamicBucket {
	b := ratelimit.NewBucketWithQuantum(time.Second, rate, rate)
	d := &DynamicBucket{}
	d.v.Store(b)
	return d
}

func (d *DynamicBucket) Get() *ratelimit.Bucket {
	return d.v.Load().(*ratelimit.Bucket)
}

func (d *DynamicBucket) Update(rate int64) {
	if rate <= 0 {
		d.v.Store((*ratelimit.Bucket)(nil))
		return
	}
	newB := ratelimit.NewBucketWithQuantum(time.Second, rate, rate)
	d.v.Store(newB)
}

func NewRateLimitWriter(writer buf.Writer, limiter *DynamicBucket) buf.Writer {
	return &Writer{
		writer:  writer,
		limiter: limiter,
		done:    make(chan struct{}),
	}
}

func (w *Writer) Close() error {
	w.closeOnce.Do(func() { close(w.done) })
	return common.Close(w.writer)
}

func (w *Writer) WriteMultiBuffer(mb buf.MultiBuffer) error {
	select {
	case <-w.done:
		buf.ReleaseMulti(mb)
		return io.ErrClosedPipe
	default:
	}
	limiter := w.limiter.Get()
	if limiter != nil {
		if delay := limiter.Take(int64(mb.Len())); delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-w.done:
				buf.ReleaseMulti(mb)
				return io.ErrClosedPipe
			}
		}
	}
	select {
	case <-w.done:
		buf.ReleaseMulti(mb)
		return io.ErrClosedPipe
	default:
	}
	return w.writer.WriteMultiBuffer(mb)
}
