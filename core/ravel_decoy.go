package core

import (
	"time"

	"github.com/YeJianbo/ravel-go/gateway"
	panel "github.com/YeJianbo/ravel/api/v2board"
)

func newRavelFrameDecoy(config *panel.RavelSettings) (gateway.DecoyHandler, error) {
	if config.WebSocketMasquerade == "" {
		return nil, nil
	}
	// Only the configured endpoint determines the dial destination. Incoming
	// authority/path values must not turn this into an unauthenticated proxy.
	return gateway.NewWebSocketDecoy(gateway.WebSocketDecoyConfig{
		URL:              config.WebSocketMasquerade,
		MaxMessageSize:   config.Settings.MaxMessage,
		HandshakeTimeout: time.Duration(config.Settings.HandshakeTimeout) * time.Second,
	})
}
