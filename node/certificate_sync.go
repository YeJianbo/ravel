package node

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/pem"
	"time"

	panel "github.com/YeJianbo/ravel/api/v2board"
	log "github.com/sirupsen/logrus"
)

func (c *Controller) captureSelfCertificate() string {
	if c.info == nil || c.info.Security != panel.Tls || c.info.Common == nil ||
		c.info.Common.CertInfo == nil || c.info.Common.CertInfo.CertMode != "self" {
		return ""
	}
	cert := c.info.Common.CertInfo
	certificateWriteMu.Lock()
	pair, err := tls.LoadX509KeyPair(cert.CertFile, cert.KeyFile)
	certificateWriteMu.Unlock()
	if err != nil || len(pair.Certificate) == 0 {
		log.WithField("tag", c.tag).Error("Certificate sync cannot load a matching local certificate/key pair")
		return ""
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: pair.Certificate[0]}))
}

// Start and nodeInfoMonitor serialize this work with runtimeMu. Keep failures
// outside the service/task error path: publication is not listener availability.
func (c *Controller) syncCertificate(ctx context.Context) {
	if c.certificateReloadPending || c.apiClient == nil || ctx.Err() != nil {
		return
	}
	token := c.apiClient.CertificateSyncToken(c.info)
	if token == "" || c.certificatePublic == "" {
		return
	}
	certificate := c.certificatePublic
	identity := sha256.Sum256([]byte(token + "\x00" + certificate))
	if identity == c.certificateSynced {
		return
	}
	// Reserve half the remaining poll budget so an optional report cannot itself
	// consume the task deadline and trigger the existing timeout/reload path.
	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline)/2 < timeout {
		timeout = time.Until(deadline) / 2
	}
	if timeout <= 0 {
		return
	}
	reportCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	newToken, err := c.apiClient.ReportCertificate(reportCtx, c.info, certificate)
	if err != nil {
		log.WithField("tag", c.tag).WithError(err).Error("Certificate sync failed; will retry on the next poll")
		return
	}
	if newToken != "" {
		c.certificateSynced = sha256.Sum256([]byte(newToken + "\x00" + certificate))
		log.WithField("tag", c.tag).Info("Published node self-signed certificate")
	}
}
