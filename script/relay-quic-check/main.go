// relay-quic-check verifies QUIC transport, not subscriber authentication.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	quic "github.com/apernet/quic-go"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: relay-quic-check host:port ...")
		os.Exit(2)
	}
	failed := false
	for _, address := range os.Args[1:] {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		started := time.Now()
		// Only diagnose whether the endpoint speaks QUIC; send no user credentials.
		connection, err := quic.DialAddr(ctx, address, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h3"}}, &quic.Config{HandshakeIdleTimeout: 5 * time.Second})
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
