// relay-quic-check verifies QUIC transport, not subscriber authentication.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"time"

	quic "github.com/apernet/quic-go"
)

func main() {
	serverName := flag.String("server-name", "", "TLS server name when dialing an IP address")
	caFile := flag.String("ca", "", "additional trusted CA certificate PEM file")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: relay-quic-check [-server-name name] [-ca certificate.pem] host:port ...")
		os.Exit(2)
	}
	tlsConfig, err := relayTLSConfig(*serverName, *caFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	failed := false
	for _, address := range flag.Args() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		started := time.Now()
		// Verify transport identity without sending subscriber credentials.
		connection, err := quic.DialAddr(ctx, address, tlsConfig.Clone(), &quic.Config{HandshakeIdleTimeout: 5 * time.Second})
		if err != nil {
			fmt.Printf("%s FAIL %v\n", address, err)
			failed = true
		} else {
			fmt.Printf("%s QUIC_OK %s\n", address, time.Since(started).Round(time.Millisecond))
			_ = connection.CloseWithError(0, "diagnostic complete")
		}
		cancel()
	}
	if failed {
		os.Exit(1)
	}
}

func relayTLSConfig(serverName, caFile string) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: serverName, NextProtos: []string{"h3"}}
	if caFile == "" {
		return config, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate: %w", err)
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA file contains no valid certificates")
	}
	config.RootCAs = roots
	return config, nil
}
