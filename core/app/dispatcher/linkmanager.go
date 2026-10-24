package dispatcher

import (
	"io"
	"net"
	sync "sync"
	"sync/atomic"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

type ManagedWriter struct {
	writer  buf.Writer
	manager *LinkManager
	counter *atomic.Int64
}

func (w *ManagedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if !w.manager.beginIO(nil) {
		buf.ReleaseMulti(mb)
		return io.ErrClosedPipe
	}
	defer w.manager.active.Done()
	w.counter.Add(int64(mb.Len()))
	return w.writer.WriteMultiBuffer(mb)
}

type managedReader struct {
	reader     buf.Reader
	manager    *LinkManager
	counter    *atomic.Int64
	connection net.Conn
}

func (r *managedReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if !r.manager.beginIO(r) {
		return nil, io.ErrClosedPipe
	}
	defer func() {
		r.manager.mu.Lock()
		r.manager.readers[r]--
		if r.manager.readers[r] == 0 {
			delete(r.manager.readers, r)
		}
		r.manager.mu.Unlock()
		r.manager.active.Done()
	}()
	mb, err := r.reader.ReadMultiBuffer()
	r.counter.Add(int64(mb.Len()))
	return mb, err
}

func (r *managedReader) Interrupt() {
	common.Interrupt(r.reader)
	if r.connection != nil {
		_ = r.connection.Close()
	}
}
func (r *managedReader) Close() error {
	if r.connection != nil {
		defer r.connection.Close()
	}
	return common.Close(r.reader)
}

func (w *ManagedWriter) Close() error {
	w.manager.RemoveWriter(w)
	return common.Close(w.writer)
}

type LinkManager struct {
	nodeTag   string
	links     map[*ManagedWriter]buf.Reader
	mu        sync.RWMutex
	closed    bool
	active    sync.WaitGroup
	closeOnce sync.Once
	readers   map[*managedReader]int
}

func (m *LinkManager) beginIO(reader *managedReader) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	m.active.Add(1)
	if reader != nil {
		if m.readers == nil {
			m.readers = make(map[*managedReader]int)
		}
		m.readers[reader]++
	}
	return true
}

func (m *LinkManager) AddLink(writer *ManagedWriter, reader buf.Reader) {
	m.mu.Lock()
	closed := m.closed
	if !closed {
		m.links[writer] = reader
	}
	m.mu.Unlock()
	if closed {
		common.Close(writer.writer)
		common.Interrupt(reader)
	}
}

func (m *LinkManager) RemoveWriter(writer *ManagedWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		delete(m.links, writer)
	}
}

func (m *LinkManager) CloseAll() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		links := m.links
		m.links = make(map[*ManagedWriter]buf.Reader)
		readers := make([]*managedReader, 0, len(m.readers))
		for reader := range m.readers {
			readers = append(readers, reader)
		}
		m.mu.Unlock()
		for w, r := range links {
			common.Close(w.writer)
			common.Interrupt(r)
		}
		// A writer may have closed and left the link map while its reader is still exiting.
		for _, reader := range readers {
			reader.Interrupt()
		}
		// No new I/O can enter after closed is set; count updates finish before returning.
		m.active.Wait()
	})
}
