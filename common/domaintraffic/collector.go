package domaintraffic

import (
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

type Record struct {
	Domain   string `json:"domain"`
	Upload   int64  `json:"upload"`
	Download int64  `json:"download"`
}
type counterValues struct {
	up, down atomic.Int64
}

type Counter struct {
	values atomic.Pointer[counterValues]
	refs   int
}
type bucket struct {
	mu      sync.Mutex
	enabled atomic.Bool
	removed atomic.Bool
	entries map[string]*Counter
}

var mu sync.RWMutex
var nodes = map[string]*bucket{}

func Remove(tag string) {
	mu.Lock()
	b := nodes[tag]
	if b != nil {
		b.removed.Store(true)
		b.enabled.Store(false)
		delete(nodes, tag)
	}
	mu.Unlock()
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.enabled.Store(false)
	for _, c := range b.entries {
		c.values.Store(nil)
	}
	clear(b.entries)
}

func Enable(tag string, enabled bool) {
	for {
		mu.Lock()
		b := nodes[tag]
		if b == nil {
			b = &bucket{entries: map[string]*Counter{}}
			nodes[tag] = b
		}
		mu.Unlock()
		b.mu.Lock()
		// A removed generation cannot be revived by a racing configuration update.
		if b.removed.Load() {
			b.mu.Unlock()
			continue
		}
		setEnabled(b, enabled)
		b.mu.Unlock()
		return
	}
}

func setEnabled(b *bucket, enabled bool) {
	if b.enabled.Load() == enabled {
		return
	}
	b.enabled.Store(enabled)
	for domain, c := range b.entries {
		if enabled {
			c.values.Store(&counterValues{})
		} else {
			// Detach this generation: an in-flight Add cannot reach a later enable.
			c.values.Store(nil)
			if c.refs == 0 {
				delete(b.entries, domain)
			}
		}
	}
}

func lookup(tag string) *bucket {
	mu.RLock()
	b := nodes[tag]
	mu.RUnlock()
	return b
}

// Only domain names are collected: never URLs, user identifiers or IP addresses.
func Acquire(tag, domain string) (*Counter, func()) {
	b := lookup(tag)
	if b == nil || !b.enabled.Load() {
		return nil, func() {}
	}
	domain = strings.ToLower(strings.TrimSuffix(domain, "."))
	if len(domain) > 253 || !strings.Contains(domain, ".") || net.ParseIP(domain) != nil {
		return nil, func() {}
	}
	for _, c := range domain {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
			return nil, func() {}
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.removed.Load() || !b.enabled.Load() {
		return nil, func() {}
	}
	c := b.entries[domain]
	if c == nil {
		if len(b.entries) >= 4096 {
			return nil, func() {}
		}
		c = &Counter{}
		c.values.Store(&counterValues{})
		b.entries[domain] = c
	}
	c.refs++
	return c, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		c.refs--
		if !b.enabled.Load() && c.refs == 0 && b.entries[domain] == c {
			delete(b.entries, domain)
		}
	}
}
func (c *Counter) Add(up, down int64) {
	if c != nil {
		if values := c.values.Load(); values != nil {
			values.up.Add(up)
			values.down.Add(down)
		}
	}
}

func Drain(tag string) []Record {
	return DrainLimited(tag, int(^uint(0)>>1))
}

// Restore only unsubmitted counters; acknowledged/retry batches stay separate.
func Restore(tag string, records []Record) {
	for _, record := range records {
		if record.Upload < 0 || record.Download < 0 {
			continue
		}
		counter, release := Acquire(tag, record.Domain)
		counter.Add(record.Upload, record.Download)
		release()
	}
}

// Keep unsent counters for a later batch; the estimate includes JSON field overhead.
func DrainLimited(tag string, maxBytes int) []Record {
	b := lookup(tag)
	if b == nil || !b.enabled.Load() {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.removed.Load() || !b.enabled.Load() {
		return nil
	}
	var result []Record
	size := 0
	for domain, c := range b.entries {
		cost := len(domain) + 100
		if size+cost > maxBytes {
			continue
		}
		values := c.values.Load()
		if values == nil {
			continue
		}
		up, down := values.up.Swap(0), values.down.Swap(0)
		if up > 0 || down > 0 {
			size += cost
			result = append(result, Record{domain, up, down})
		}
		if c.refs == 0 {
			delete(b.entries, domain)
		}
	}
	return result
}
