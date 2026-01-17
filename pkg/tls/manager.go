package tls

import (
	"context"
	"crypto/tls"
	"log"
	"net/http"
	"os"
	"time"

	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

type Config struct {
	Enabled     bool
	Domains     []string
	Email       string
	CertDir     string
	RenewalDays int // Note: Not used by autocert, kept for documentation
	Staging     bool
}

type Manager struct {
	certManager *autocert.Manager
	config      Config
}

func NewManager(cfg Config) *Manager {
	if !cfg.Enabled {
		return nil
	}

	if cfg.CertDir == "" {
		cfg.CertDir = "/data/certs"
	}

	if cfg.RenewalDays == 0 {
		cfg.RenewalDays = 10
	}

	os.MkdirAll(cfg.CertDir, 0700)

	certManager := &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		Email:      cfg.Email,
		HostPolicy: autocert.HostWhitelist(cfg.Domains...),
		Cache:      autocert.DirCache(cfg.CertDir),
	}

	if cfg.Staging {
		certManager.Client = &acme.Client{
			DirectoryURL: "https://acme-staging-v02.api.letsencrypt.org/directory",
		}
	}

	log.Printf("TLS Manager initialized for domains: %v", cfg.Domains)
	log.Printf("Certificates will be stored in: %s", cfg.CertDir)
	log.Printf("Auto-renewal handled by autocert (approximately 30 days before expiry)")

	return &Manager{
		certManager: certManager,
		config:      cfg,
	}
}

func (m *Manager) GetTLSConfig() *tls.Config {
	if m == nil {
		return nil
	}

	return &tls.Config{
		GetCertificate: m.certManager.GetCertificate,
		MinVersion:     tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}
}

func (m *Manager) HTTPHandler(h http.Handler) http.Handler {
	if m == nil {
		return h
	}
	return m.certManager.HTTPHandler(h)
}

func (m *Manager) StartAutoRenewal(ctx context.Context) {
	if m == nil {
		return
	}

	log.Println("TLS: Auto-renewal service started (managed by autocert)")

	// Optional: Monitor certificate expiry every 24 hours
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Println("TLS: Auto-renewal service stopped")
				return
			case <-ticker.C:
				// Check certificate status
				for _, domain := range m.config.Domains {
					if cert, err := m.certManager.Cache.Get(ctx, domain); err == nil && len(cert) > 0 {
						// Parse first certificate to check expiry
						// This is informational only - autocert handles actual renewal
						log.Printf("TLS: Certificate for %s exists in cache", domain)
					}
				}
			}
		}
	}()
}
