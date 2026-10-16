package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/YeJianbo/ravel/common/file"
)

var certificateWriteMu sync.Mutex

func (c *Controller) renewCertTask(_ context.Context) error {
	l, err := NewLego(c.info.Common.CertInfo)
	if err != nil {
		log.WithField("tag", c.tag).Info("new lego error: ", err)
		return nil
	}
	err = l.RenewCert()
	if err != nil {
		log.WithField("tag", c.tag).Info("renew cert error: ", err)
		return nil
	}
	return nil
}

func (c *Controller) requestCert() error {
	cert := c.info.Common.CertInfo
	switch cert.CertMode {
	case "none", "":
	case "file":
		if cert.CertFile == "" || cert.KeyFile == "" {
			return fmt.Errorf("cert file path or key file path not exist")
		}
	case "dns", "http":
		if cert.CertFile == "" || cert.KeyFile == "" {
			return fmt.Errorf("cert file path or key file path not exist")
		}
		if file.IsExist(cert.CertFile) && file.IsExist(cert.KeyFile) {
			return nil
		}
		l, err := NewLego(cert)
		if err != nil {
			return fmt.Errorf("create lego object error: %s", err)
		}
		err = l.CreateCert()
		if err != nil {
			return fmt.Errorf("create lego cert error: %s", err)
		}
	case "self":
		if cert.CertFile == "" || cert.KeyFile == "" {
			return fmt.Errorf("cert file path or key file path not exist")
		}
		if file.IsExist(cert.CertFile) && file.IsExist(cert.KeyFile) {
			return nil
		}
		err := generateSelfSslCertificate(
			cert.CertDomain,
			cert.CertFile,
			cert.KeyFile)
		if err != nil {
			return fmt.Errorf("generate self cert error: %s", err)
		}
	case "remote":
		if cert.CertFile == "" || cert.KeyFile == "" {
			return fmt.Errorf("cert file path or key file path not exist")
		}
		if cert.TlsCert == "" && cert.TlsKey == "" {
			// Legacy/offline configurations can reuse a valid locally stored pair.
			if _, err := tls.LoadX509KeyPair(cert.CertFile, cert.KeyFile); err != nil {
				return fmt.Errorf("load cached remote certificate: %w", err)
			}
			return nil
		}
		if cert.TlsCert == "" || cert.TlsKey == "" {
			return fmt.Errorf("tls cert or tls key not exist")
		}
		if err := writeCertificatePair(cert.CertFile, cert.KeyFile, []byte(cert.TlsCert), []byte(cert.TlsKey)); err != nil {
			return fmt.Errorf("write remote certificate: %w", err)
		}

	default:
		return fmt.Errorf("unsupported certmode: %s", cert.CertMode)
	}
	return nil
}

func generateSelfSslCertificate(domain, certPath, keyPath string) error {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return fmt.Errorf("generate private key: %w", err)
	}
	tmpl := &x509.Certificate{
		Version:      3,
		SerialNumber: big.NewInt(time.Now().Unix()),
		Subject: pkix.Name{
			CommonName: domain,
		},
		DNSNames:              []string{domain},
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(30, 0, 0),
	}
	cert, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert,
	})
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	return writeCertificatePair(certPath, keyPath, certPEM, keyPEM)
}

func writeCertificatePair(certPath, keyPath string, certPEM, keyPEM []byte) error {
	absCertPath, err := filepath.Abs(certPath)
	if err != nil {
		return err
	}
	absKeyPath, err := filepath.Abs(keyPath)
	if err != nil {
		return err
	}
	if absCertPath == absKeyPath {
		return errors.New("certificate and private key paths must differ")
	}
	if _, err := tls.X509KeyPair(certPEM, keyPEM); err != nil {
		return fmt.Errorf("validate certificate pair: %w", err)
	}
	// Multiple node controllers may renew the same certificate files.
	certificateWriteMu.Lock()
	defer certificateWriteMu.Unlock()
	oldKey, readErr := os.ReadFile(keyPath)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return fmt.Errorf("read previous private key: %w", readErr)
	}
	oldCert, certReadErr := os.ReadFile(certPath)
	if certReadErr != nil && !errors.Is(certReadErr, os.ErrNotExist) {
		return fmt.Errorf("read previous certificate: %w", certReadErr)
	}
	if readErr == nil && certReadErr == nil && bytes.Equal(oldKey, keyPEM) && bytes.Equal(oldCert, certPEM) {
		return nil
	}
	if err := file.WriteAtomic(keyPath, keyPEM, 0600); err != nil {
		return fmt.Errorf("write private key: %w", err)
	}
	if err := file.WriteAtomic(certPath, certPEM, 0644); err != nil {
		// Each rename is atomic, but the pair is not: restore the old key on a write error.
		var rollbackErr error
		if readErr == nil {
			rollbackErr = file.WriteAtomic(keyPath, oldKey, 0600)
		} else {
			rollbackErr = os.Remove(keyPath)
		}
		if rollbackErr != nil {
			return errors.Join(fmt.Errorf("write certificate: %w", err), fmt.Errorf("restore previous private key: %w", rollbackErr))
		}
		return fmt.Errorf("write certificate: %w", err)
	}
	return nil
}
