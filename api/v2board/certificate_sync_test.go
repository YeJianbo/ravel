package panel

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YeJianbo/ravel/conf"
)

func TestCertificateSyncRequestAndTokenOnlyConfigChanges(t *testing.T) {
	token, etag := "sync-before", `"before"`
	port, posts := 443, 0
	var certificate string
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer machine-v1.test" || r.URL.Query().Get("node_id") != "7" || r.URL.Query().Get("token") != "" {
			t.Error("incorrect scoped authentication or node query")
		}
		if r.URL.Path == "/api/v2/server/config" {
			w.Header().Set("ETag", etag)
			if r.Header.Get("If-None-Match") == etag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"protocol": "vless", "tls": 1, "server_port": port,
				"tls_settings":           map[string]any{"cert_mode": "self"},
				"base_config":            map[string]any{"pull_interval": 60, "push_interval": 60},
				"certificate_sync_token": token,
			})
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/v2/server/certificate" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("incorrect certificate request")
		}
		posts++
		body, _ := io.ReadAll(r.Body)
		var payload map[string]string
		if json.Unmarshal(body, &payload) != nil || len(payload) != 2 || payload["certificate"] != certificate || payload["config_token"] != token || strings.Contains(string(body), "PRIVATE KEY") {
			t.Error("certificate request included incorrect fields or private material")
		}
		token, etag = "sync-after", `"after"`
		_, _ = io.WriteString(w, `{"accepted":true,"changed":true,"config_token":"sync-after"}`)
	}))
	defer server.Close()
	certificate = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.TLS.Certificates[0].Certificate[0]}))
	c := certificateTestClient(t, server)
	info, err := c.GetNodeInfo(context.Background())
	if err != nil || info == nil || c.CertificateSyncToken(info) != "sync-before" {
		t.Fatalf("initial config: %v", err)
	}
	initialHash := c.responseBodyHash
	if next, err := c.ReportCertificate(context.Background(), info, certificate); err != nil || next != "sync-after" || c.CertificateSyncToken(info) != next {
		t.Fatalf("report acknowledgement: %v", err)
	}
	if next, err := c.GetNodeInfo(context.Background()); err != nil || next != nil || c.responseBodyHash != initialHash || c.nodeEtag != `"after"` {
		t.Fatalf("first post-report GET caused reload or failed to refresh ETag: %v", err)
	}
	if next, err := c.GetNodeInfo(context.Background()); err != nil || next != nil {
		t.Fatalf("post-report 304 caused reload: %v", err)
	}
	token, etag = "sync-from-get", `"get-only"`
	if next, err := c.GetNodeInfo(context.Background()); err != nil || next != nil || c.CertificateSyncToken(info) != token {
		t.Fatalf("GET-only token refresh caused reload or lost token: %v", err)
	}
	token, etag = "", `"removed"`
	if next, err := c.GetNodeInfo(context.Background()); err != nil || next != nil || c.CertificateSyncToken(info) != "" {
		t.Fatalf("token removal caused reload or retained publication permission: %v", err)
	}
	token, etag, port = "new-config", `"changed"`, 8443
	if next, err := c.GetNodeInfo(context.Background()); err != nil || next == nil || next.Common.ServerPort != port {
		t.Fatalf("real config change was hidden: %v", err)
	}
	if next, err := c.ReportCertificate(context.Background(), info, certificate); err != nil || next != "" || posts != 1 {
		t.Fatal("old config was allowed to publish after a newer config arrived")
	}
}

func certificateTestClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	zero := 0
	c, err := New(&conf.NodeConfig{APIHost: server.URL, NodeID: 7, Key: "machine-v1.test", RetryCount: &zero})
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	c.client.SetTLSClientConfig(&tls.Config{RootCAs: roots})
	t.Cleanup(c.CloseIdleConnections)
	return c
}

func TestCertificateSyncCachedTokenRecoversAfterConflict(t *testing.T) {
	const etag = `"current"`
	const config = `{"protocol":"vless","tls":1,"tls_settings":{"cert_mode":"self"},"base_config":{"pull_interval":60,"push_interval":60},"certificate_sync_token":"current"}`
	fullGets, cachedGets, posts := 0, 0, 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v2/server/config" {
			w.Header().Set("ETag", etag)
			if r.Header.Get("If-None-Match") == etag {
				cachedGets++
				w.WriteHeader(http.StatusNotModified)
				return
			}
			fullGets++
			_, _ = io.WriteString(w, config)
			return
		}
		posts++
		var payload struct {
			ConfigToken string `json:"config_token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if payload.ConfigToken != "current" {
			w.WriteHeader(http.StatusConflict)
			return
		}
		_, _ = io.WriteString(w, `{"accepted":true,"changed":false,"config_token":"current"}`)
	}))
	defer server.Close()
	c := certificateTestClient(t, server)
	// Reproduce a snapshot with the old Common token but the post-report ETag.
	info, err := parseNodeInfo([]byte(strings.Replace(config, `"certificate_sync_token":"current"`, `"certificate_sync_token":"stale"`, 1)), server.URL, 7)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := nodeConfigHash([]byte(config))
	if err != nil {
		t.Fatal(err)
	}
	c.nodeEtag, c.responseBodyHash = etag, hash
	if c.CertificateSyncToken(info) != "stale" {
		t.Fatal("cached stale token was not restored")
	}
	if next, err := c.GetNodeInfo(context.Background()); err != nil || next != nil || c.CertificateSyncToken(info) != "stale" || cachedGets != 1 {
		t.Fatalf("initial 304 did not reproduce stale-token cache: %v", err)
	}
	certificate := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.TLS.Certificates[0].Certificate[0]}))
	if _, err := c.ReportCertificate(context.Background(), info, certificate); err == nil || !strings.Contains(err.Error(), "HTTP 409") || c.nodeEtag != "" || c.responseBodyHash != hash {
		t.Fatalf("conflict did not invalidate only the ETag: %v", err)
	}
	if next, err := c.GetNodeInfo(context.Background()); err != nil || next != nil || c.CertificateSyncToken(info) != "current" || c.nodeEtag != etag || c.responseBodyHash != hash || fullGets != 1 {
		t.Fatalf("full GET did not refresh token without reload: %v", err)
	}
	if token, err := c.ReportCertificate(context.Background(), info, certificate); err != nil || token != "current" || posts != 2 {
		t.Fatalf("publication did not recover after token refresh: %v", err)
	}
	if next, err := c.GetNodeInfo(context.Background()); err != nil || next != nil || cachedGets != 2 {
		t.Fatalf("conditional GET did not resume after recovery: %v", err)
	}
}

func TestCertificateSyncEligibilityAndPublicPEMOnly(t *testing.T) {
	posts := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts++
		_, _ = io.WriteString(w, `{"accepted":true,"changed":true,"config_token":"new"}`)
	}))
	defer server.Close()
	certificate := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.TLS.Certificates[0].Certificate[0]}))
	for _, kind := range []string{"no-token", "http", "legacy", "file", "non-tls", "private-key"} {
		t.Run(kind, func(t *testing.T) {
			c := certificateTestClient(t, server)
			info := &NodeInfo{Security: Tls, Common: &CommonNode{Tls: Tls, CertificateSyncToken: "sync", CertInfo: &CertInfo{CertMode: "self"}}}
			payload := certificate
			switch kind {
			case "no-token":
				info.Common.CertificateSyncToken = ""
			case "http":
				c.APIHost = strings.Replace(c.APIHost, "https:", "http:", 1)
			case "legacy":
				c.Token = "legacy-key"
			case "file":
				info.Common.CertInfo.CertMode = "file"
			case "non-tls":
				info.Security = None
			case "private-key":
				payload += "-----BEGIN PRIVATE KEY-----\nc2VjcmV0\n-----END PRIVATE KEY-----\n"
			}
			_, err := c.ReportCertificate(context.Background(), info, payload)
			if (kind == "private-key") != (err != nil) {
				t.Fatalf("unexpected report result: %v", err)
			}
		})
	}
	if posts != 0 {
		t.Fatalf("ineligible or private-key requests sent: %d", posts)
	}
}

func TestCertificateSyncRejectsUnsafeAcknowledgementsAndRedirects(t *testing.T) {
	status, body := http.StatusConflict, "sync-secret certificate-contents"
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected = true }))
	defer target.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	defer server.Close()
	c := certificateTestClient(t, server)
	info := &NodeInfo{Security: Tls, Common: &CommonNode{Tls: Tls, CertificateSyncToken: "sync-secret", CertInfo: &CertInfo{CertMode: "self"}}}
	certificate := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.TLS.Certificates[0].Certificate[0]}))
	for _, code := range []int{http.StatusConflict, http.StatusTemporaryRedirect, http.StatusOK} {
		status = code
		if code == http.StatusOK {
			body = `{"data":true,"config_token":"not-an-accepted-ack"}`
		}
		if _, err := c.ReportCertificate(context.Background(), info, certificate); err == nil || strings.Contains(err.Error(), body) || strings.Contains(err.Error(), "sync-secret") {
			t.Fatal("unsafe acknowledgement accepted or response data leaked")
		}
	}
	if redirected || c.CertificateSyncToken(info) != "sync-secret" {
		t.Fatal("redirect followed or failed report changed token")
	}
}
