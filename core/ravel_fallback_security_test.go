package core

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestRavelFileFallbackRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "public")
	if err := os.Mkdir(root, 0755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		filepath.Join(root, "index.html"): "cover",
		filepath.Join(root, "asset.txt"):  "asset",
		filepath.Join(base, "outside"):    "secret",
	} {
		if err := os.WriteFile(name, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	handler, err := newRavelSafeFileHandler(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, rangeHeader, body string
		status                          int
	}{
		{http.MethodGet, "/", "", "cover", 200},
		{http.MethodHead, "/asset.txt", "", "", 200},
		{http.MethodGet, "/asset.txt", "bytes=1-3", "sse", 206},
		{http.MethodGet, "/../outside", "", "404 page not found\n", 404},
		{http.MethodGet, "/%00", "", "Forbidden\n", 403},
		{http.MethodPost, "/", "", "Method Not Allowed\n", 405},
	} {
		t.Run(tc.method+tc.path+tc.rangeHeader, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, "http://cover.example"+tc.path, nil)
			request.Header.Set("Range", tc.rangeHeader)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != tc.status || response.Body.String() != tc.body {
				t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
			}
		})
	}
	if err := os.Symlink(filepath.Join(base, "outside"), filepath.Join(root, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://cover.example/link", nil))
	if response.Code != http.StatusForbidden || response.Body.String() == "secret" {
		t.Fatalf("symlink exposed: status=%d body=%q", response.Code, response.Body.String())
	}
}
