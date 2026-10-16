package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

const embeddedRelayMaxConnections = 32

type embeddedRelay struct {
	mu          sync.Mutex
	listeners   []io.Closer
	connections map[net.Conn]struct{}
	udpPeers    map[string]*embeddedUDPPeer
	udpPending  int
	listening   map[string]bool
	active      chan struct{}
	closed      bool
	ctx         context.Context
	cancel      context.CancelFunc
}

type embeddedUDPPeer struct {
	conn net.Conn
}

func startEmbeddedRelay(nodes []string) (*embeddedRelay, error) {
	listeners := relayListenersFromNodes(nodes)
	ctx, cancel := context.WithCancel(context.Background())
	relay := &embeddedRelay{
		connections: make(map[net.Conn]struct{}),
		udpPeers:    make(map[string]*embeddedUDPPeer),
		listening:   make(map[string]bool, len(listeners)),
		active:      make(chan struct{}, embeddedRelayMaxConnections),
		ctx:         ctx,
		cancel:      cancel,
	}
	for _, rule := range listeners {
		address := net.JoinHostPort(rule.ListenHost, strconv.Itoa(rule.ListenPort))
		if rule.ListenHost == "0.0.0.0" || rule.ListenHost == "::" {
			address = ":" + strconv.Itoa(rule.ListenPort)
		}
		key := rule.Protocol + ":" + strconv.Itoa(rule.ListenPort)
		switch rule.Protocol {
		case "tcp":
			listener, err := net.Listen("tcp", address)
			if err != nil {
				relay.Close()
				return nil, fmt.Errorf("listen %s: %w", address, err)
			}
			relay.listeners = append(relay.listeners, listener)
			relay.listening[key] = true
		case "udp":
			listener, err := net.ListenPacket("udp", address)
			if err != nil {
				relay.Close()
				return nil, fmt.Errorf("listen UDP %s: %w", address, err)
			}
			relay.listeners = append(relay.listeners, listener)
			relay.listening[key] = true
		default:
			relay.Close()
			return nil, fmt.Errorf("unsupported embedded relay protocol %q", rule.Protocol)
		}
	}
	for index, rule := range listeners {
		switch listener := relay.listeners[index].(type) {
		case net.Listener:
			go relay.serveTCP(listener, rule)
		case net.PacketConn:
			go relay.serveUDP(listener, rule, index)
		}
	}
	return relay, nil
}

func (r *embeddedRelay) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	r.cancel()
	closers := append([]io.Closer(nil), r.listeners...)
	for conn := range r.connections {
		closers = append(closers, conn)
	}
	for _, peer := range r.udpPeers {
		closers = append(closers, peer.conn)
	}
	r.mu.Unlock()
	for _, closer := range closers {
		_ = closer.Close()
	}
}

func (r *embeddedRelay) serveTCP(listener net.Listener, rule relayListener) {
	target := net.JoinHostPort(rule.TargetHost, strconv.Itoa(rule.TargetPort))
	for {
		client, err := listener.Accept()
		if err != nil {
			return
		}
		select {
		case r.active <- struct{}{}:
			go func() {
				defer func() { <-r.active }()
				r.forwardTCP(client, target)
			}()
		default:
			_ = client.Close()
		}
	}
}

func (r *embeddedRelay) forwardTCP(client net.Conn, target string) {
	defer client.Close()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.connections[client] = struct{}{}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.connections, client)
		r.mu.Unlock()
	}()
	dialer := net.Dialer{Timeout: 5 * time.Second}
	upstream, err := dialer.DialContext(r.ctx, "tcp", target)
	if err != nil {
		return
	}
	defer upstream.Close()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.connections[upstream] = struct{}{}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.connections, upstream)
		r.mu.Unlock()
	}()
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(upstream, client)
		if conn, ok := upstream.(*net.TCPConn); ok {
			_ = conn.CloseWrite()
		}
		done <- struct{}{}
	}()
	_, _ = io.Copy(client, upstream)
	if conn, ok := client.(*net.TCPConn); ok {
		_ = conn.CloseWrite()
	}
	<-done
}

func (r *embeddedRelay) serveUDP(listener net.PacketConn, rule relayListener, index int) {
	target := net.JoinHostPort(rule.TargetHost, strconv.Itoa(rule.TargetPort))
	buffer := make([]byte, 65535)
	for {
		count, address, err := listener.ReadFrom(buffer)
		if err != nil {
			return
		}
		key := strconv.Itoa(index) + ":" + address.String()
		r.mu.Lock()
		peer := r.udpPeers[key]
		create := peer == nil && !r.closed && len(r.udpPeers)+r.udpPending < embeddedRelayMaxConnections
		if create {
			// Reserve capacity before releasing the lock for DNS and dialing.
			r.udpPending++
		}
		r.mu.Unlock()
		if create {
			dialer := net.Dialer{Timeout: 3 * time.Second}
			conn, dialErr := dialer.DialContext(r.ctx, "udp", target)
			r.mu.Lock()
			r.udpPending--
			if dialErr == nil && !r.closed {
				peer = &embeddedUDPPeer{conn: conn}
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
				r.udpPeers[key] = peer
				go r.forwardUDPReplies(listener, address, key, peer)
			} else if conn != nil {
				_ = conn.Close()
			}
			r.mu.Unlock()
		}
		if peer != nil {
			_ = peer.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
			if _, err := peer.conn.Write(buffer[:count]); err != nil {
				_ = peer.conn.Close()
				continue
			}
			_ = peer.conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		}
	}
}

func (r *embeddedRelay) forwardUDPReplies(listener net.PacketConn, address net.Addr, key string, peer *embeddedUDPPeer) {
	defer func() {
		r.mu.Lock()
		if r.udpPeers[key] == peer {
			delete(r.udpPeers, key)
		}
		r.mu.Unlock()
		_ = peer.conn.Close()
	}()
	buffer := make([]byte, 65535)
	for {
		count, err := peer.conn.Read(buffer)
		if err != nil {
			return
		}
		if _, err := listener.WriteTo(buffer[:count], address); err != nil {
			return
		}
	}
}
