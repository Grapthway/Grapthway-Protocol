package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	LogRetentionDays       int
	NodeOperatorPrivateKey string
	BootstrapPeers         []string

	// TLS/HTTPS Configuration
	TLSEnabled bool
	TLSDomains []string
	TLSEmail   string
	TLSCertDir string
	TLSStaging bool
	HTTPSPort  string
	HTTPPort   string
}

func NewConfig() *Config {
	retentionDays, err := strconv.Atoi(os.Getenv("LOG_RETENTION_DAYS"))
	if err != nil {
		retentionDays = 7
	}

	privateKey := os.Getenv("NODE_OPERATOR_PRIVATE_KEY")
	peersStr := os.Getenv("BOOTSTRAP_PEERS")
	var bootstrapPeers []string
	if peersStr != "" {
		bootstrapPeers = strings.Split(peersStr, ",")
	}

	domainsStr := os.Getenv("TLS_DOMAINS")
	var domains []string
	if domainsStr != "" {
		domains = strings.Split(domainsStr, ",")
		for i := range domains {
			domains[i] = strings.TrimSpace(domains[i])
		}
	}

	tlsEnabled := getEnvAsBool("TLS_ENABLED", false)

	return &Config{
		LogRetentionDays:       retentionDays,
		NodeOperatorPrivateKey: privateKey,
		BootstrapPeers:         bootstrapPeers,
		TLSEnabled:             tlsEnabled,
		TLSDomains:             domains,
		TLSEmail:               os.Getenv("TLS_EMAIL"),
		TLSCertDir:             getEnvOrDefault("TLS_CERT_DIR", "/data/certs"),
		TLSStaging:             getEnvAsBool("TLS_STAGING", false),
		HTTPSPort:              getEnvOrDefault("HTTPS_PORT", "443"),
		HTTPPort:               getEnvOrDefault("HTTP_PORT", "80"),
	}
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		b, _ := strconv.ParseBool(val)
		return b
	}
	return defaultVal
}
