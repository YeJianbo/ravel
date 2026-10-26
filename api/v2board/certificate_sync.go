package panel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type certificateSyncState struct {
	sync.Mutex
	info  *NodeInfo
	token string
}

// The token tracks panel-side pins, not runtime configuration. Its rotation must
// update authentication without rebuilding a healthy listener.
func nodeConfigHash(body []byte) (string, error) {
	var config map[string]json.RawMessage
	if err := json.Unmarshal(body, &config); err != nil {
		return "", errors.New("decode node config for hashing failed")
	}
	delete(config, "certificate_sync_token")
	normalized, err := json.Marshal(config)
	if err != nil {
		return "", errors.New("encode node config for hashing failed")
	}
	hash := sha256.Sum256(normalized)
	return hex.EncodeToString(hash[:]), nil
}

// Called with certificateSync held, after validating the full response.
func (c *Client) refreshCertificateSyncToken(node *NodeInfo) {
	if c.certificateSync.info == nil {
		c.certificateSync.info = node
	}
	c.certificateSync.token = node.Common.CertificateSyncToken
}

func (c *Client) certificateSyncToken(info *NodeInfo) string {
	if info == nil || info.Common == nil {
		return ""
	}
	// An offline snapshot can start before the first successful config fetch.
	if c.certificateSync.info == nil {
		c.certificateSync.info = info
		c.certificateSync.token = info.Common.CertificateSyncToken
	}
	if c.certificateSync.info != info || info.Security != Tls || info.Common.Tls != Tls ||
		info.Common.CertInfo == nil || info.Common.CertInfo.CertMode != "self" ||
		!strings.HasPrefix(c.Token, "machine-v1.") || len(c.Token) <= len("machine-v1.") {
		return ""
	}
	origin, err := url.Parse(c.APIHost)
	if err != nil || origin.Scheme != "https" || origin.Hostname() == "" || origin.User != nil {
		return ""
	}
	return c.certificateSync.token
}

func (c *Client) CertificateSyncToken(info *NodeInfo) string {
	c.certificateSync.Lock()
	defer c.certificateSync.Unlock()
	return c.certificateSyncToken(info)
}

// ReportCertificate accepts only a public leaf certificate. It reuses the panel
// transport, but never follows redirects or logs response/request bodies.
func (c *Client) ReportCertificate(ctx context.Context, info *NodeInfo, certificate string) (string, error) {
	c.certificateSync.Lock()
	defer c.certificateSync.Unlock()
	token := c.certificateSyncToken(info)
	if token == "" {
		return "", nil
	}
	block, rest := pem.Decode([]byte(certificate))
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return "", errors.New("certificate sync requires one public PEM certificate")
	}
	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		return "", errors.New("certificate sync received an invalid certificate")
	}
	certificate = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: block.Bytes}))
	body, err := json.Marshal(struct {
		Certificate string `json:"certificate"`
		ConfigToken string `json:"config_token"`
	}{certificate, token})
	if err != nil {
		return "", errors.New("encode certificate sync request failed")
	}
	endpoint, err := url.Parse(strings.TrimRight(c.APIHost, "/") + "/api/v2/server/certificate")
	if err != nil {
		return "", errors.New("invalid certificate sync endpoint")
	}
	query := endpoint.Query()
	query.Set("node_id", strconv.Itoa(c.NodeId))
	query.Set("node_type", "ravel")
	endpoint.RawQuery = query.Encode()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return "", errors.New("create certificate sync request failed")
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", panelUserAgent())
	client := *c.client.GetClient()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(req)
	if err != nil {
		// Transport errors can contain redirect URLs or arbitrary peer data.
		if ctx.Err() != nil {
			return "", fmt.Errorf("certificate sync request: %w", ctx.Err())
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return "", errors.New("certificate sync transport timed out")
		}
		var verificationErr *tls.CertificateVerificationError
		if errors.As(err, &verificationErr) {
			return "", errors.New("certificate sync TLS verification failed")
		}
		return "", errors.New("certificate sync transport failed")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusConflict {
		// Cached runtime info can retain an older token than its saved ETag.
		// Force a full config fetch, preserving the hash to avoid a reload.
		c.nodeEtag = ""
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("certificate sync returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, ackResponseMaxBytes+1))
	if err != nil || len(data) > ackResponseMaxBytes {
		return "", errors.New("read certificate sync acknowledgement failed")
	}
	var ack struct {
		Accepted    bool   `json:"accepted"`
		ConfigToken string `json:"config_token"`
	}
	if json.Unmarshal(data, &ack) != nil || !ack.Accepted || ack.ConfigToken == "" {
		return "", errors.New("panel did not acknowledge certificate sync with a config token")
	}
	c.certificateSync.token = ack.ConfigToken
	return ack.ConfigToken, nil
}
