// File: pkg/common/http.go
package common

import (
	"net/http"
	"sync"
	"time"
)

var (
	defaultHTTPClient     *http.Client
	defaultHTTPClientOnce sync.Once
)

// GetHTTPClient returns a shared HTTP client with default 10s timeout
func GetHTTPClient() *http.Client {
	defaultHTTPClientOnce.Do(func() {
		defaultHTTPClient = &http.Client{
			Transport: createTransport(),
			Timeout:   10 * time.Second,
		}
	})
	return defaultHTTPClient
}

// GetHTTPClientWithTimeout creates a NEW HTTP client with custom timeout
// Each client gets its own transport to avoid timeout conflicts
func GetHTTPClientWithTimeout(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	return &http.Client{
		Transport: createTransport(), // ✅ Fresh transport per client
		Timeout:   timeout,           // ✅ This controls the overall request timeout
	}
}

// GetTransportWithTimeout creates a custom transport for reverse proxies
// Use this ONLY for httputil.ReverseProxy where you can't set Client.Timeout
func GetTransportWithTimeout(timeout time.Duration) *http.Transport {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	transport := createTransport()
	// For reverse proxy, we need ResponseHeaderTimeout since there's no Client.Timeout
	transport.ResponseHeaderTimeout = timeout
	return transport
}

// createTransport creates a new HTTP transport with optimized settings
// ⚠️  Does NOT set ResponseHeaderTimeout - let Client.Timeout control this
func createTransport() *http.Transport {
	return &http.Transport{
		// Connection pooling
		MaxIdleConns:        100, // Total idle connections
		MaxIdleConnsPerHost: 20,  // Per-host idle connections
		MaxConnsPerHost:     0,   // Unlimited active connections

		// Timeouts
		IdleConnTimeout:       90 * time.Second, // Close idle connections after 90s
		TLSHandshakeTimeout:   10 * time.Second, // TLS handshake timeout
		ExpectContinueTimeout: 1 * time.Second,  // Expect: 100-continue timeout

		// Performance
		DisableKeepAlives:  false, // Enable connection reuse
		DisableCompression: false, // Enable gzip
		ForceAttemptHTTP2:  true,  // Prefer HTTP/2
	}
}

// ParseTimeout parses duration string with fallback to default
// Supports formats like: "500ms", "10s", "5m", "1h"
func ParseTimeout(ttlStr string, defaultTimeout time.Duration) time.Duration {
	if ttlStr == "" {
		return defaultTimeout
	}

	duration, err := time.ParseDuration(ttlStr)
	if err != nil {
		// Log warning but don't crash - use default
		return defaultTimeout
	}

	return duration
}
