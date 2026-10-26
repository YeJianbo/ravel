package node

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	panel "github.com/YeJianbo/ravel/api/v2board"
	"github.com/YeJianbo/ravel/conf"
	"github.com/YeJianbo/ravel/core"
	"github.com/YeJianbo/ravel/limiter"
)

var certificateTestRootsOnce sync.Once

func TestCertificateSyncStartupRetryAndReload(t *testing.T) {
	posts, port := 0, certificateTestPort(t)
	token, etag := "before", `"before"`
	certPath, keyPath := filepath.Join(t.TempDir(), "self.pem"), filepath.Join(t.TempDir(), "self.key")
	if err := generateSelfSslCertificate("node.test", certPath, keyPath); err != nil {
		t.Fatal(err)
	}
	publicPEM, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	var configBody []byte
	config := func() []byte {
		body, err := json.Marshal(map[string]any{
			"protocol": "vless", "listen_ip": "127.0.0.1", "server_port": port, "network": "tcp", "tls": 1,
			"tls_settings":           map[string]any{"cert_mode": "self", "cert_file": certPath, "key_file": keyPath, "server_name": "node.test"},
			"base_config":            map[string]any{"pull_interval": 3600, "push_interval": 3600},
			"certificate_sync_token": token,
		})
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v2/server/config":
			w.Header().Set("ETag", etag)
			if r.Header.Get("If-None-Match") == etag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			_, _ = w.Write(configBody)
		case "/api/v1/server/UniProxy/user":
			_, _ = io.WriteString(w, `{"users":[{"id":7,"uuid":"3c2c92d0-d032-4c55-834e-9a5c8f57383d"}]}`)
		case "/api/v1/server/UniProxy/alivelist":
			_, _ = io.WriteString(w, `{"alive":{}}`)
		case "/api/v2/server/certificate":
			posts++
			body, _ := io.ReadAll(r.Body)
			var payload map[string]string
			if json.Unmarshal(body, &payload) != nil || len(payload) != 2 || payload["certificate"] != string(publicPEM) || payload["config_token"] != token || strings.Contains(string(body), "PRIVATE KEY") {
				t.Error("incorrect publication payload or private material uploaded")
			}
			if posts == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			token, etag = "after", `"after"`
			_, _ = io.WriteString(w, `{"accepted":true,"changed":true,"config_token":"after"}`)
		default:
			t.Errorf("unexpected request path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	// Test-only trust for the local HTTPS panel. Production TLS verification is
	// unchanged; no insecure transport or exported testing hook is needed.
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",x509usefallbackroots=1")
	certificateTestRootsOnce.Do(func() { x509.SetFallbackRoots(roots) })
	configBody = config()
	zero := 0
	nodeConfig := conf.NodeConfig{APIHost: server.URL, NodeID: 7, Key: "machine-v1.test", RetryCount: &zero}
	api, err := panel.New(&nodeConfig)
	if err != nil {
		t.Fatal(err)
	}
	info, err := api.GetNodeInfo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	limiter.Init()
	runtime := core.New(conf.New())
	runtime.ReloadCh = make(chan struct{}, 1)
	if err := runtime.Start([]*panel.NodeInfo{info}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	c := NewController(api, &nodeConfig, info)
	if err := c.Start(runtime); err != nil || posts != 1 {
		t.Fatalf("report failure prevented startup or report wasn't attempted: posts=%d err=%v", posts, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	// Keep this integration test deterministic; invoke the existing poll path.
	c.nodeInfoMonitorPeriodic.Close()
	c.userReportPeriodic.Close()
	assertCertificateListener(t, port, publicPEM)
	// Replacing files after startup must not publish a future listener identity.
	originalKey, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	wrongCert, wrongKey := filepath.Join(t.TempDir(), "other.pem"), filepath.Join(t.TempDir(), "other.key")
	if err := generateSelfSslCertificate("other.test", wrongCert, wrongKey); err != nil {
		t.Fatal(err)
	}
	futureCert, _ := os.ReadFile(wrongCert)
	futureKey, _ := os.ReadFile(wrongKey)
	if err := writeCertificatePair(certPath, keyPath, futureCert, futureKey); err != nil {
		t.Fatal(err)
	}
	if err := c.nodeInfoMonitor(context.Background()); err != nil || posts != 2 {
		t.Fatalf("poll did not retry failed startup publication: posts=%d err=%v", posts, err)
	}
	configBody = config()
	if err := c.nodeInfoMonitor(context.Background()); err != nil || posts != 2 || len(runtime.ReloadCh) != 0 {
		t.Fatalf("token/ETag update reloaded or repeated publication: posts=%d err=%v", posts, err)
	}
	if err := c.nodeInfoMonitor(context.Background()); err != nil || posts != 2 {
		t.Fatalf("unchanged-token/certificate poll republished: %v", err)
	}
	if err := writeCertificatePair(certPath, keyPath, publicPEM, originalKey); err != nil {
		t.Fatal(err)
	}
	// A bad local key must never activate a new publication snapshot.
	info.Common.CertInfo.KeyFile = wrongKey
	if c.captureSelfCertificate() != "" {
		t.Fatal("mismatched certificate/key was captured")
	}
	info.Common.CertInfo.KeyFile = keyPath
	port, token, etag = certificateTestPort(t), "new-runtime", `"new-runtime"`
	configBody = config()
	if err := c.nodeInfoMonitor(context.Background()); err != nil || posts != 2 || len(runtime.ReloadCh) != 1 {
		t.Fatalf("real config didn't reload first or old cert published: posts=%d err=%v", posts, err)
	}
	c.syncCertificate(context.Background())
	if posts != 2 {
		t.Fatal("old controller published after requesting reload")
	}
}

func certificateTestPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

func assertCertificateListener(t *testing.T, port int, certificate []byte) {
	t.Helper()
	block, _ := pem.Decode(certificate)
	parsed, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", fmt.Sprintf("127.0.0.1:%d", port), &tls.Config{RootCAs: roots, ServerName: "node.test"})
	if err != nil {
		t.Fatalf("node listener was unavailable after publication failure: %v", err)
	}
	_ = conn.Close()
}
