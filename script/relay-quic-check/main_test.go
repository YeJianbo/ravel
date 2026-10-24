package main

import (
	"crypto/tls"
	"encoding/pem"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestRelayTLSVerification(t *testing.T) {
	config, err := relayTLSConfig("cover.example", "")
	if err != nil || config.InsecureSkipVerify || config.ServerName != "cover.example" || config.MinVersion != tls.VersionTLS13 {
		t.Fatalf("invalid verification config: %+v, %v", config, err)
	}
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	file := filepath.Join(t.TempDir(), "ca.pem")
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(file, certificate, 0600); err != nil {
		t.Fatal(err)
	}
	config, err = relayTLSConfig("example.com", file)
	if err != nil || config.RootCAs == nil || config.InsecureSkipVerify {
		t.Fatalf("custom CA unavailable: %v", err)
	}
	if err := os.WriteFile(file, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := relayTLSConfig("example.com", file); err == nil {
		t.Fatal("invalid CA accepted")
	}
}
