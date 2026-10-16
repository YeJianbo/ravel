package core

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YeJianbo/ravel-go"
	"github.com/YeJianbo/ravel-go/carrier"
	"github.com/gobwas/ws"
	panel "github.com/YeJianbo/ravel/api/v2board"
)

func TestRavelRuntimeRealWebSocketDecoy(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RequestURI() != "/application?mode=cover" {
			http.NotFound(w, r)
			return
		}
		conn, buffered, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		socket := carrier.NewWebSocket(buffered.Reader, conn, conn, ws.StateServerSide, 65536)
		frame, err := socket.ReadFrame()
		if err == nil {
			_ = socket.WriteFrame(frame)
		}
	}))
	defer origin.Close()
	decoy, err := newRavelFrameDecoy(&panel.RavelSettings{WebSocketMasquerade: "ws" + strings.TrimPrefix(origin.URL, "http") + "/application?mode=cover"})
	if err != nil {
		t.Fatal(err)
	}
	certificate := httptest.NewTLSServer(http.NotFoundHandler())
	tlsConfig := certificate.TLS.Clone()
	certificate.Close()
	runtime := &ravelRuntime{
		config: ravelRuntimeConfig{listen: "127.0.0.1:0", tlsConfig: tlsConfig, authority: "example.com", path: "/socket",
			frameDecoy: decoy, fallback: http.NotFoundHandler(),
			settings: panel.RavelProtocolSettings{ClaimRetention: 60, ConnectionLifetime: 60, DrainTimeout: 1, HandshakeTimeout: 2}},
		dialer: &net.Dialer{}, users: make(map[string]panel.UserInfo),
		activeAuth: make(map[ravel.AuthContext]int), authUID: make(map[ravel.AuthContext]int),
	}
	if err := runtime.replaceUsers(nil); err != nil {
		t.Fatal(err)
	}
	defer runtime.close()
	for _, frame := range []carrier.Message{
		{OpCode: ws.OpBinary, Payload: []byte("invalid credential"), Final: true},
		{OpCode: ws.OpPing, Payload: []byte("application ping"), Final: true},
		{OpCode: ws.OpText, Payload: []byte("fragment"), Fragmented: true},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		socket, _, err := carrier.DialH2(ctx, carrier.H2ClientConfig{
			Authority: "example.com", Path: "/socket",
			DialTLSContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}}}).DialContext(ctx, network, runtime.listener.Addr().String())
			},
		})
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if err = socket.WriteFrame(frame); err != nil {
			socket.Abort()
			cancel()
			t.Fatal(err)
		}
		reply, err := socket.ReadFrame()
		socket.Abort()
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if reply.OpCode != frame.OpCode || reply.Final != frame.Final || string(reply.Payload) != string(frame.Payload) {
			t.Fatalf("decoy altered frame: got %#v want %#v", reply, frame)
		}
	}
}
