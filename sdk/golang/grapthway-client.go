// grapthway-client.go
package grapthway

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/json-iterator/go"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/secp256k1"
)

// ========================================
// NODE POOL MANAGER
// ========================================

type Node struct {
	URL       string
	Latency   time.Duration
	Failures  int
	LastCheck time.Time
	IsHealthy bool
	mu        sync.RWMutex
}

type NodePoolManager struct {
	nodes               []*Node
	maxFailures         int
	healthCheckInterval time.Duration
	pingTimeout         time.Duration
	lastHealthCheck     time.Time
	mu                  sync.RWMutex
}

func NewNodePoolManager(nodeURLs []string) *NodePoolManager {
	nodes := make([]*Node, len(nodeURLs))
	for i, nodeURL := range nodeURLs {
		nodes[i] = &Node{
			URL:       strings.TrimSuffix(nodeURL, "/"),
			Latency:   time.Duration(math.MaxInt64),
			Failures:  0,
			LastCheck: time.Time{},
			IsHealthy: true,
		}
	}

	return &NodePoolManager{
		nodes:               nodes,
		maxFailures:         3,
		healthCheckInterval: 30 * time.Second,
		pingTimeout:         5 * time.Second,
		lastHealthCheck:     time.Time{},
	}
}

func NewNodePoolManagerFromEnv() (*NodePoolManager, error) {
	var urls []string

	// Try GRAPTHWAY_NODE_URL first (backward compatibility)
	if nodeURL := os.Getenv("GRAPTHWAY_NODE_URL"); nodeURL != "" {
		urls = append(urls, nodeURL)
	}

	// Then try GRAPTHWAY_NODE_URL_1, GRAPTHWAY_NODE_URL_2, etc.
	index := 1
	for {
		envKey := fmt.Sprintf("GRAPTHWAY_NODE_URL_%d", index)
		if nodeURL := os.Getenv(envKey); nodeURL != "" {
			urls = append(urls, nodeURL)
			index++
		} else {
			break
		}
	}

	if len(urls) == 0 {
		return nil, errors.New("no Grapthway node URLs found. Set GRAPTHWAY_NODE_URL or GRAPTHWAY_NODE_URL_1, GRAPTHWAY_NODE_URL_2, etc.")
	}

	fmt.Printf("✅ Loaded %d Grapthway node(s) from environment\n", len(urls))
	return NewNodePoolManager(urls), nil
}

func (npm *NodePoolManager) pingNode(node *Node) time.Duration {
	startTime := time.Now()
	client := &http.Client{
		Timeout: npm.pingTimeout,
	}

	resp, err := client.Get(fmt.Sprintf("%s/public/gateway-status", node.URL))
	if err != nil {
		node.mu.Lock()
		node.Failures++
		node.Latency = time.Duration(math.MaxInt64)
		if node.Failures >= npm.maxFailures {
			node.IsHealthy = false
			fmt.Printf("⚠️ Node %s marked as unhealthy after %d failures\n", node.URL, node.Failures)
		}
		node.mu.Unlock()
		return time.Duration(math.MaxInt64)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		node.mu.Lock()
		node.Failures++
		node.Latency = time.Duration(math.MaxInt64)
		if node.Failures >= npm.maxFailures {
			node.IsHealthy = false
		}
		node.mu.Unlock()
		return time.Duration(math.MaxInt64)
	}

	latency := time.Since(startTime)
	node.mu.Lock()
	node.Latency = latency
	node.Failures = 0
	node.IsHealthy = true
	node.LastCheck = time.Now()
	node.mu.Unlock()

	return latency
}

func (npm *NodePoolManager) UpdateLatencies() {
	fmt.Println("🔍 Checking latency for all nodes...")

	var wg sync.WaitGroup
	for _, node := range npm.nodes {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			npm.pingNode(n)
		}(node)
	}
	wg.Wait()

	// Sort nodes by latency (healthy nodes first)
	npm.mu.Lock()
	sort.Slice(npm.nodes, func(i, j int) bool {
		npm.nodes[i].mu.RLock()
		npm.nodes[j].mu.RLock()
		defer npm.nodes[i].mu.RUnlock()
		defer npm.nodes[j].mu.RUnlock()

		if npm.nodes[i].IsHealthy && !npm.nodes[j].IsHealthy {
			return true
		}
		if !npm.nodes[i].IsHealthy && npm.nodes[j].IsHealthy {
			return false
		}
		return npm.nodes[i].Latency < npm.nodes[j].Latency
	})
	npm.lastHealthCheck = time.Now()
	npm.mu.Unlock()

	// Log results
	for index, node := range npm.nodes {
		node.mu.RLock()
		status := "✅"
		if !node.IsHealthy {
			status = "❌"
		}
		latency := "TIMEOUT"
		if node.Latency != time.Duration(math.MaxInt64) {
			latency = fmt.Sprintf("%dms", node.Latency.Milliseconds())
		}
		fmt.Printf(" %s #%d: %s - %s\n", status, index+1, node.URL, latency)
		node.mu.RUnlock()
	}
}

func (npm *NodePoolManager) GetBestNode() (*Node, error) {
	npm.mu.RLock()
	timeSinceLastCheck := time.Since(npm.lastHealthCheck)
	npm.mu.RUnlock()

	// Perform health check if needed
	if timeSinceLastCheck > npm.healthCheckInterval {
		npm.UpdateLatencies()
	}

	npm.mu.RLock()
	defer npm.mu.RUnlock()

	// Return first healthy node (already sorted by latency)
	for _, node := range npm.nodes {
		node.mu.RLock()
		isHealthy := node.IsHealthy
		node.mu.RUnlock()

		if isHealthy {
			return node, nil
		}
	}

	// All nodes are down, try the least-failed one
	fmt.Println("⚠️ All nodes unhealthy, attempting least-failed node")
	if len(npm.nodes) > 0 {
		return npm.nodes[0], nil
	}

	return nil, errors.New("no nodes available")
}

func (npm *NodePoolManager) MarkNodeFailed(nodeURL string) {
	npm.mu.RLock()
	defer npm.mu.RUnlock()

	for _, node := range npm.nodes {
		if node.URL == nodeURL {
			node.mu.Lock()
			node.Failures++
			if node.Failures >= npm.maxFailures {
				node.IsHealthy = false
				fmt.Printf("⚠️ Node %s marked unhealthy after request failure\n", node.URL)
			}
			node.mu.Unlock()
			return
		}
	}
}

func (npm *NodePoolManager) GetAllNodeURLs() []string {
	npm.mu.RLock()
	defer npm.mu.RUnlock()

	urls := make([]string, len(npm.nodes))
	for i, node := range npm.nodes {
		urls[i] = node.URL
	}
	return urls
}

func (npm *NodePoolManager) GetAllNodeDomains() []string {
	npm.mu.RLock()
	defer npm.mu.RUnlock()

	domains := make([]string, len(npm.nodes))
	for i, node := range npm.nodes {
		parsedURL, err := url.Parse(node.URL)
		if err != nil {
			domains[i] = node.URL
		} else {
			domains[i] = parsedURL.Hostname()
		}
	}
	return domains
}

// ========================================
// UTILITY FUNCTIONS
// ========================================

func normalizeConfig(data interface{}) interface{} {
	if data == nil {
		return nil
	}

	switch v := data.(type) {
	case map[string]interface{}:
		normalized := make(map[string]interface{})
		for key, value := range v {
			if value == nil {
				continue
			}
			if str, ok := value.(string); ok && str == "" {
				continue
			}
			if arr, ok := value.([]interface{}); ok && len(arr) == 0 {
				continue
			}
			if obj, ok := value.(map[string]interface{}); ok && len(obj) == 0 {
				continue
			}
			normalized[key] = normalizeConfig(value)
		}
		return normalized
	case []interface{}:
		normalized := make([]interface{}, len(v))
		for i, item := range v {
			normalized[i] = normalizeConfig(item)
		}
		return normalized
	default:
		return data
	}
}

func marshalCanonical(data interface{}) (string, error) {
	normalized := normalizeConfig(data)

	if normalized == nil {
		return "null", nil
	}

	switch v := normalized.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		pairs := make([]string, 0, len(keys))
		for _, key := range keys {
			valStr, err := marshalCanonical(v[key])
			if err != nil {
				return "", err
			}
			pairs = append(pairs, fmt.Sprintf(`"%s":%s`, key, valStr))
		}
		return fmt.Sprintf("{%s}", strings.Join(pairs, ",")), nil

	case []interface{}:
		items := make([]string, len(v))
		for i, item := range v {
			itemStr, err := marshalCanonical(item)
			if err != nil {
				return "", err
			}
			items[i] = itemStr
		}
		return fmt.Sprintf("[%s]", strings.Join(items, ",")), nil

	default:
		b, err := json.Marshal(v)
		return string(b), err
	}
}

// ========================================
// ACCOUNT MANAGEMENT
// ========================================

type Account struct {
	ID           string    `json:"id"`
	PrivateKey   string    `json:"privateKey"`
	Address      string    `json:"address"`
	PublicKeyHex string    `json:"publicKeyHex"`
	Label        string    `json:"label"`
	CreatedAt    time.Time `json:"createdAt"`
}

type AccountManager struct {
	accounts        []*Account
	selectedAccount *Account
	mu              sync.RWMutex
}

func NewAccountManager() *AccountManager {
	return &AccountManager{
		accounts: make([]*Account, 0),
	}
}

func (am *AccountManager) AddAccount(privateKeyHex, label string) (*Account, error) {
	// Remove 0x prefix if present
	privateKeyHex = strings.TrimPrefix(privateKeyHex, "0x")

	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	// Get public key - this returns crypto.PublicKey (interface)
	publicKey := privateKey.Public()
	// Type assert to *ecdsa.PublicKey (concrete type)
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("cannot assert type: publicKey is not of type *ecdsa.PublicKey")
	}
	publicKeyBytes := crypto.FromECDSAPub(publicKeyECDSA)
	publicKeyHex := hex.EncodeToString(publicKeyBytes)

	// Calculate address
	pubKeyBytes := publicKeyBytes[1:] // Remove 0x04 prefix
	hash := sha256.Sum256(pubKeyBytes)
	address := "0x" + hex.EncodeToString(hash[len(hash)-20:])

	if label == "" {
		label = fmt.Sprintf("Account %s...", address[:8])
	}

	account := &Account{
		ID:           fmt.Sprintf("%d", time.Now().UnixNano()),
		PrivateKey:   privateKeyHex,
		Address:      address,
		PublicKeyHex: publicKeyHex,
		Label:        label,
		CreatedAt:    time.Now(),
	}

	am.mu.Lock()
	am.accounts = append(am.accounts, account)
	if am.selectedAccount == nil {
		am.selectedAccount = account
	}
	am.mu.Unlock()

	return account, nil
}

func (am *AccountManager) RemoveAccount(accountID string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	for i, account := range am.accounts {
		if account.ID == accountID {
			am.accounts = append(am.accounts[:i], am.accounts[i+1:]...)
			if am.selectedAccount != nil && am.selectedAccount.ID == accountID {
				if len(am.accounts) > 0 {
					am.selectedAccount = am.accounts[0]
				} else {
					am.selectedAccount = nil
				}
			}
			return nil
		}
	}
	return errors.New("account not found")
}

func (am *AccountManager) SelectAccount(accountID string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	for _, account := range am.accounts {
		if account.ID == accountID {
			am.selectedAccount = account
			return nil
		}
	}
	return errors.New("account not found")
}

func (am *AccountManager) GetCurrentAccount() (*Account, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	if am.selectedAccount == nil {
		return nil, errors.New("no account selected")
	}
	return am.selectedAccount, nil
}

func (am *AccountManager) UpdateAccountLabel(accountID, label string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	for _, account := range am.accounts {
		if account.ID == accountID {
			account.Label = label
			return nil
		}
	}
	return errors.New("account not found")
}

func (am *AccountManager) ClearAccounts() {
	am.mu.Lock()
	defer am.mu.Unlock()

	am.accounts = make([]*Account, 0)
	am.selectedAccount = nil
}

// ========================================
// GRAPTHWAY CLIENT
// ========================================

type ClientConfig struct {
	NodeURLs   []string
	NodeURL    string
	PrivateKey string
	Label      string
}

type GrapthwayClient struct {
	nodePool       *NodePoolManager
	accountManager *AccountManager
	httpClient     *http.Client
}

func NewGrapthwayClient(config *ClientConfig) (*GrapthwayClient, error) {
	var npm *NodePoolManager
	var err error

	if config != nil && len(config.NodeURLs) > 0 {
		npm = NewNodePoolManager(config.NodeURLs)
	} else if config != nil && config.NodeURL != "" {
		npm = NewNodePoolManager([]string{config.NodeURL})
	} else {
		npm, err = NewNodePoolManagerFromEnv()
		if err != nil {
			return nil, err
		}
	}

	// Perform initial health check
	go npm.UpdateLatencies()

	client := &GrapthwayClient{
		nodePool:       npm,
		accountManager: NewAccountManager(),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	// Add account if private key provided
	if config != nil && config.PrivateKey != "" {
		_, err = client.accountManager.AddAccount(config.PrivateKey, config.Label)
		if err != nil {
			return nil, fmt.Errorf("failed to add account: %w", err)
		}
	}

	return client, nil
}

func (gc *GrapthwayClient) GetAccountManager() *AccountManager {
	return gc.accountManager
}

func (gc *GrapthwayClient) getCurrentIdentity() (*Account, *ecdsa.PrivateKey, error) {
	account, err := gc.accountManager.GetCurrentAccount()
	if err != nil {
		return nil, nil, err
	}

	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(account.PrivateKey, "0x"))
	if err != nil {
		return nil, nil, fmt.Errorf("invalid private key: %w", err)
	}

	return account, privateKey, nil
}

// signData signs the input data string and returns a DER-encoded hex string signature.
func (gc *GrapthwayClient) signData(data string, privateKey *ecdsa.PrivateKey) (string, error) {
	// Calculate SHA256 hash of data
	hash := sha256.Sum256([]byte(data))

	// Sign the hash to get raw signature (r, s, v)
	signature, err := secp256k1.Sign(hash[:], crypto.FromECDSA(privateKey))
	if err != nil {
		return "", err
	}

	// The signature returned by secp256k1.Sign is [R || S || V] (65 bytes)
	// We need to convert R and S to DER encoding like elliptic.ec in JS

	r := new(big.Int).SetBytes(signature[:32])
	s := new(big.Int).SetBytes(signature[32:64])

	// Encode to DER
	derSig := encodeECDSASignatureDER(r, s)

	return hex.EncodeToString(derSig), nil
}

// encodeECDSASignatureDER encodes (r, s) to DER format.
func encodeECDSASignatureDER(r, s *big.Int) []byte {
	// DER encoding structure:
	// 0x30 total_length 0x02 r_length r_bytes 0x02 s_length s_bytes
	rBytes := r.Bytes()
	sBytes := s.Bytes()

	// Helper to prepend 0x00 if high bit set (to denote positive integers)
	prependZero := func(b []byte) []byte {
		if len(b) > 0 && b[0]&0x80 != 0 {
			return append([]byte{0x00}, b...)
		}
		return b
	}

	rBytes = prependZero(rBytes)
	sBytes = prependZero(sBytes)

	length := 2 + len(rBytes) + 2 + len(sBytes) // total length of r and s parts + 4 (2 bytes tag+length each)

	der := make([]byte, 0, length+2)
	der = append(der, 0x30) // SEQUENCE tag
	der = append(der, byte(length))
	der = append(der, 0x02) // INTEGER tag for r
	der = append(der, byte(len(rBytes)))
	der = append(der, rBytes...)
	der = append(der, 0x02) // INTEGER tag for s
	der = append(der, byte(len(sBytes)))
	der = append(der, sBytes...)

	return der
}

type AuthType string

const (
	AuthTypeUser   AuthType = "user"
	AuthTypeAdmin  AuthType = "admin"
	AuthTypePublic AuthType = "public"
)

type RequestOptions struct {
	Method     string
	Body       interface{}
	AuthType   AuthType
	PathPrefix string
	MaxRetries int
}

func (gc *GrapthwayClient) CreateAuthHeaders(bodyString string, isAdmin bool) (map[string]string, error) {
	account, privateKey, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	headers := map[string]string{
		"Content-Type":               "application/json",
		"X-Grapthway-Developer-ID":   account.Address,
		"X-Grapthway-User-Address":   account.Address,
		"X-Grapthway-User-PublicKey": account.PublicKeyHex,
	}

	// Sign the body or empty string
	dataToSign := bodyString
	if dataToSign == "" {
		dataToSign = ""
	}

	signature, err := gc.signData(dataToSign, privateKey)
	if err != nil {
		return nil, err
	}
	headers["X-Grapthway-User-Signature"] = signature

	// Add admin headers if needed
	if isAdmin {
		adminSignature, err := gc.signData(account.Address, privateKey)
		if err != nil {
			return nil, err
		}
		headers["X-Grapthway-Admin-Address"] = account.Address
		headers["X-Grapthway-Admin-Signature"] = adminSignature
	}

	return headers, nil
}

func (gc *GrapthwayClient) request(endpoint string, options *RequestOptions) (map[string]interface{}, error) {
	if options == nil {
		options = &RequestOptions{}
	}

	if options.Method == "" {
		options.Method = "GET"
	}
	if options.AuthType == "" {
		options.AuthType = AuthTypeUser
	}
	if options.MaxRetries == 0 {
		options.MaxRetries = 3
	}

	prefix := "/api/v1"
	if options.PathPrefix != "" {
		prefix = options.PathPrefix
	} else if options.AuthType == AuthTypeAdmin {
		prefix = "/admin"
	} else if options.AuthType == AuthTypePublic {
		prefix = "/public"
	}

	var bodyString string
	if options.Body != nil {
		bodyBytes, err := json.Marshal(options.Body)
		if err != nil {
			return nil, err
		}
		bodyString = string(bodyBytes)
	}

	var headers map[string]string
	var err error
	if options.AuthType == AuthTypePublic {
		headers = map[string]string{"Content-Type": "application/json"}
	} else {
		headers, err = gc.CreateAuthHeaders(bodyString, options.AuthType == AuthTypeAdmin)
		if err != nil {
			return nil, err
		}
	}

	var lastError error
	for attemptCount := 1; attemptCount <= options.MaxRetries; attemptCount++ {
		node, err := gc.nodePool.GetBestNode()
		if err != nil {
			return nil, err
		}

		fullURL := fmt.Sprintf("%s%s%s", node.URL, prefix, endpoint)

		if attemptCount > 1 {
			fmt.Printf("🔄 Retry attempt %d/%d using %s\n", attemptCount, options.MaxRetries, node.URL)
		}

		var reqBody io.Reader
		if bodyString != "" {
			reqBody = bytes.NewBufferString(bodyString)
		}

		req, err := http.NewRequest(options.Method, fullURL, reqBody)
		if err != nil {
			lastError = err
			continue
		}

		for key, value := range headers {
			req.Header.Set(key, value)
		}

		resp, err := gc.httpClient.Do(req)
		if err != nil {
			lastError = err
			gc.nodePool.MarkNodeFailed(node.URL)
			fmt.Printf("⚠️ Request failed: %v\n", err)

			if attemptCount < options.MaxRetries {
				time.Sleep(time.Duration(math.Pow(2, float64(attemptCount))) * time.Second)
			}
			continue
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			lastError = err
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			lastError = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
			gc.nodePool.MarkNodeFailed(node.URL)
			fmt.Printf("⚠️ Request failed: %v\n", lastError)

			if attemptCount < options.MaxRetries {
				time.Sleep(time.Duration(math.Pow(2, float64(attemptCount))) * time.Second)
			}
			continue
		}

		if len(respBody) == 0 {
			return make(map[string]interface{}), nil
		}

		var result map[string]interface{}
		if err := json.Unmarshal(respBody, &result); err != nil {
			return nil, err
		}

		return result, nil
	}

	return nil, fmt.Errorf("all %d request attempts failed. Last error: %v", options.MaxRetries, lastError)
}

// ========================================
// NODE STATS
// ========================================

type NodeStats struct {
	URL       string    `json:"url"`
	Latency   *int64    `json:"latency"`
	IsHealthy bool      `json:"isHealthy"`
	Failures  int       `json:"failures"`
	LastCheck time.Time `json:"lastCheck"`
}

type PoolStats struct {
	Nodes        []NodeStats `json:"nodes"`
	TotalNodes   int         `json:"totalNodes"`
	HealthyNodes int         `json:"healthyNodes"`
}

func (gc *GrapthwayClient) GetNodeStats() *PoolStats {
	gc.nodePool.mu.RLock()
	defer gc.nodePool.mu.RUnlock()

	stats := &PoolStats{
		Nodes:        make([]NodeStats, len(gc.nodePool.nodes)),
		TotalNodes:   len(gc.nodePool.nodes),
		HealthyNodes: 0,
	}

	for i, node := range gc.nodePool.nodes {
		node.mu.RLock()
		var latency *int64
		if node.Latency != time.Duration(math.MaxInt64) {
			ms := node.Latency.Milliseconds()
			latency = &ms
		}
		stats.Nodes[i] = NodeStats{
			URL:       node.URL,
			Latency:   latency,
			IsHealthy: node.IsHealthy,
			Failures:  node.Failures,
			LastCheck: node.LastCheck,
		}
		if node.IsHealthy {
			stats.HealthyNodes++
		}
		node.mu.RUnlock()
	}

	return stats
}

func (gc *GrapthwayClient) CheckNodeHealth() (*PoolStats, error) {
	gc.nodePool.UpdateLatencies()
	return gc.GetNodeStats(), nil
}

// ========================================
// CONFIGURATION MANAGEMENT - FIXED VERSION
// ========================================

func (gc *GrapthwayClient) PublishConfig(config map[string]interface{}) (map[string]interface{}, error) {
	account, privateKey, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	// Add the developer's public key to the config
	configWithPubKey := make(map[string]interface{})
	for k, v := range config {
		configWithPubKey[k] = v
	}
	configWithPubKey["developerPubKey"] = account.PublicKeyHex

	// ✅ CRITICAL: Use canonical JSON for BOTH signing and sending
	bodyString, err := marshalCanonical(configWithPubKey)
	if err != nil {
		return nil, err
	}

	// Debug: Print the canonical JSON
	fmt.Printf("📄 Publishing canonical config for '%s':\n%s\n\n", config["service"], bodyString)

	// Sign the canonical JSON string (not the map)
	configSignature, err := gc.signData(bodyString, privateKey)
	if err != nil {
		return nil, err
	}

	// Also sign for user authentication
	userSignature, err := gc.signData(bodyString, privateKey)
	if err != nil {
		return nil, err
	}

	node, err := gc.nodePool.GetBestNode()
	if err != nil {
		return nil, err
	}

	// ✅ Send the EXACT canonical string as the body
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/publish-config", node.URL), bytes.NewBufferString(bodyString))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Grapthway-Developer-ID", account.Address)
	req.Header.Set("X-Grapthway-User-Address", account.Address)
	req.Header.Set("X-Grapthway-User-PublicKey", account.PublicKeyHex)
	req.Header.Set("X-Grapthway-User-Signature", userSignature)
	req.Header.Set("X-Grapthway-Config-Signature", configSignature)

	resp, err := gc.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("failed to publish config: %d %s", resp.StatusCode, string(respBody))
	}

	fmt.Printf("✅ Successfully published config for service '%s'\n", config["service"])

	return map[string]interface{}{
		"success": true,
		"service": config["service"],
	}, nil
}

func (gc *GrapthwayClient) Heartbeat(payload map[string]interface{}) (map[string]interface{}, error) {
	account, privateKey, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	// Add the developer's public key to the payload
	payloadWithPubKey := make(map[string]interface{})
	for k, v := range payload {
		payloadWithPubKey[k] = v
	}
	payloadWithPubKey["developerPubKey"] = account.PublicKeyHex

	// ✅ CRITICAL: Use canonical JSON for BOTH signing and sending
	bodyString, err := marshalCanonical(payloadWithPubKey)
	if err != nil {
		return nil, err
	}

	// Sign the canonical JSON string
	configSignature, err := gc.signData(bodyString, privateKey)
	if err != nil {
		return nil, err
	}

	userSignature, err := gc.signData(bodyString, privateKey)
	if err != nil {
		return nil, err
	}

	node, err := gc.nodePool.GetBestNode()
	if err != nil {
		return nil, err
	}

	// ✅ Send the EXACT canonical string as the body
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/health", node.URL), bytes.NewBufferString(bodyString))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Grapthway-Developer-ID", account.Address)
	req.Header.Set("X-Grapthway-User-Address", account.Address)
	req.Header.Set("X-Grapthway-User-PublicKey", account.PublicKeyHex)
	req.Header.Set("X-Grapthway-User-Signature", userSignature)
	req.Header.Set("X-Grapthway-Config-Signature", configSignature)

	resp, err := gc.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("heartbeat failed: %d %s", resp.StatusCode, string(respBody))
	}

	return map[string]interface{}{
		"success": true,
		"service": payload["service"],
	}, nil
}

// ========================================
// WALLET OPERATIONS
// ========================================

func (gc *GrapthwayClient) GetBalance(address string) (float64, error) {
	result, err := gc.request(fmt.Sprintf("/wallet/balance?address=%s", address), &RequestOptions{
		AuthType: AuthTypePublic,
	})
	if err != nil {
		return 0, err
	}

	balance, ok := result["balance"].(float64)
	if !ok {
		return 0, errors.New("invalid balance response")
	}
	return balance, nil
}

func (gc *GrapthwayClient) GetNonce(address string) (int64, error) {
	result, err := gc.request(fmt.Sprintf("/wallet/nonce?address=%s", address), &RequestOptions{
		AuthType: AuthTypePublic,
	})
	if err != nil {
		return 0, err
	}

	nonce, ok := result["nonce"].(float64)
	if !ok {
		return 0, errors.New("invalid nonce response")
	}
	return int64(nonce), nil
}

func (gc *GrapthwayClient) Transfer(to string, amount float64) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/wallet/transfer", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"to":     to,
			"amount": amount,
			"nonce":  nonce + 1,
		},
	})
}

type TransactionHistoryOptions struct {
	Page  int
	Limit int
	Start string
	End   string
}

func (gc *GrapthwayClient) GetTransactionHistory(address string, options *TransactionHistoryOptions) (map[string]interface{}, error) {
	if options == nil {
		options = &TransactionHistoryOptions{
			Page:  1,
			Limit: 10,
		}
	}

	query := fmt.Sprintf("address=%s&page=%d&limit=%d", address, options.Page, options.Limit)
	if options.Start != "" {
		query += fmt.Sprintf("&start=%s", options.Start)
	}
	if options.End != "" {
		query += fmt.Sprintf("&end=%s", options.End)
	}

	return gc.request(fmt.Sprintf("/wallet/history?%s", query), &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

func (gc *GrapthwayClient) SetAllowance(spender string, amount float64) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/wallet/allowance/set", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"spender": spender,
			"amount":  amount,
			"nonce":   nonce + 1,
		},
	})
}

func (gc *GrapthwayClient) RemoveAllowance(spender string) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/wallet/allowance/remove", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"spender": spender,
			"nonce":   nonce + 1,
		},
	})
}

func (gc *GrapthwayClient) GetAllowance(owner, spender string) (map[string]interface{}, error) {
	return gc.request(fmt.Sprintf("/wallet/allowance?owner=%s&spender=%s", owner, spender), &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

// ========================================
// STAKING OPERATIONS
// ========================================

func (gc *GrapthwayClient) GetStakingStatus(ownerAddress string) (map[string]interface{}, error) {
	return gc.request(fmt.Sprintf("/staking/status?owner_address=%s", ownerAddress), &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

func (gc *GrapthwayClient) StakeDeposit(amount float64) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/staking/deposit", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"amount": amount,
			"nonce":  nonce + 1,
		},
	})
}

func (gc *GrapthwayClient) StakeWithdraw(amount float64) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/staking/withdraw", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"amount": amount,
			"nonce":  nonce + 1,
		},
	})
}

func (gc *GrapthwayClient) StakeAssign(nodePeerID string, amount float64) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/staking/assign", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"nodePeerId": nodePeerID,
			"amount":     amount,
			"nonce":      nonce + 1,
		},
	})
}

func (gc *GrapthwayClient) StakeUnassign(nodePeerID string) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/staking/unassign", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"nodePeerId": nodePeerID,
			"nonce":      nonce + 1,
		},
	})
}

// ========================================
// TOKEN OPERATIONS
// ========================================

type TokenMetadata struct {
	Name          string  `json:"name"`
	Ticker        string  `json:"ticker"`
	Decimals      uint8   `json:"decimals"`
	InitialSupply float64 `json:"initialSupply"`
	TokenType     string  `json:"tokenType"`
	BurnConfig    struct {
		Enabled    bool `json:"enabled"`
		ManualBurn bool `json:"manualBurn"`
	} `json:"burnConfig"`
	MintConfig struct {
		Enabled    bool `json:"enabled"`
		ManualMint bool `json:"manualMint"`
	} `json:"mintConfig"`
	Metadata string `json:"metadata"`
}

type TokenType string

const (
	FungibleToken    TokenType = "FUNGIBLE"
	NonFungibleToken TokenType = "NFT"
	RealWorldAsset   TokenType = "RWA"
)

func (gc *GrapthwayClient) CreateToken(tokenMetadata TokenMetadata) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/token/create", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"tokenMetadata": tokenMetadata,
			"nonce":         nonce + 1,
		},
	})
}

func (gc *GrapthwayClient) TransferToken(tokenAddress, to string, amount float64) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/token/transfer", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"tokenAddress": tokenAddress,
			"to":           to,
			"amount":       amount,
			"nonce":        nonce + 1,
		},
	})
}

// ========================================
// TOKEN INFO RESPONSE TYPE
// ========================================

type TokenInfoResponse struct {
	Address       string    `json:"address"`
	Name          string    `json:"name"`
	Ticker        string    `json:"ticker"`
	Decimals      uint8     `json:"decimals"`
	InitialSupply uint64    `json:"initialSupply"`
	TotalSupply   uint64    `json:"totalSupply"`
	Creator       string    `json:"creator"`
	CreatedAt     time.Time `json:"createdAt"`
	TokenType     string    `json:"tokenType"`
	BurnConfig    BurnCfg   `json:"burnConfig"`
	MintConfig    MintCfg   `json:"mintConfig"`
	ConfigLocked  bool      `json:"configLocked"`
	Metadata      string    `json:"metadata"`
}

func (gc *GrapthwayClient) GetTokenInfo(tokenAddress string) (*TokenInfoResponse, error) {
	raw, err := gc.request(
		fmt.Sprintf("/token/info?address=%s", tokenAddress),
		&RequestOptions{AuthType: AuthTypePublic},
	)
	if err != nil {
		return nil, err
	}

	bytes, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	var resp TokenInfoResponse
	if err := json.Unmarshal(bytes, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (gc *GrapthwayClient) GetTokenBalance(tokenAddress, owner string) (map[string]interface{}, error) {
	return gc.request(fmt.Sprintf("/token/balance?tokenAddress=%s&owner=%s", tokenAddress, owner), &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

// ========================================
// OWNED TOKEN RESPONSE TYPES
// ========================================

type OwnedTokensResponse struct {
	Owner  string       `json:"owner"`
	Tokens []OwnedToken `json:"tokens"`
	Count  int          `json:"count"`
}

type OwnedToken struct {
	Balance uint64 `json:"balance"`
	Token   Token  `json:"token"`
}

type Token struct {
	Address       string    `json:"address"`
	Name          string    `json:"name"`
	Ticker        string    `json:"ticker"`
	Decimals      uint8     `json:"decimals"`
	InitialSupply uint64    `json:"initialSupply"`
	TotalSupply   uint64    `json:"totalSupply"`
	Creator       string    `json:"creator"`
	CreatedAt     time.Time `json:"createdAt"`
	TokenType     string    `json:"tokenType"`
	BurnConfig    BurnCfg   `json:"burnConfig"`
	MintConfig    MintCfg   `json:"mintConfig"`
	ConfigLocked  bool      `json:"configLocked"`
	Metadata      string    `json:"metadata"`
}

type BurnCfg struct {
	Enabled       bool   `json:"enabled"`
	BurnRatePerTx uint64 `json:"burnRatePerTx"`
	ManualBurn    bool   `json:"manualBurn"`
}

type MintCfg struct {
	Enabled       bool   `json:"enabled"`
	MintRatePerTx uint64 `json:"mintRatePerTx"`
	ManualMint    bool   `json:"manualMint"`
}

func (gc *GrapthwayClient) GetOwnedTokens(owner string) (*OwnedTokensResponse, error) {
	raw, err := gc.request(
		fmt.Sprintf("/token/owned?owner=%s", owner),
		&RequestOptions{AuthType: AuthTypeUser},
	)
	if err != nil {
		return nil, err
	}

	// Marshal back to JSON then unmarshal into strong type
	bytes, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	var resp OwnedTokensResponse
	if err := json.Unmarshal(bytes, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (gc *GrapthwayClient) SetTokenAllowance(tokenAddress, spender string, amount float64) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/token/allowance/set", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"tokenAddress": tokenAddress,
			"spender":      spender,
			"amount":       amount,
			"nonce":        nonce + 1,
		},
	})
}

func (gc *GrapthwayClient) DeleteTokenAllowance(tokenAddress, spender string) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/token/allowance/delete", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"tokenAddress": tokenAddress,
			"spender":      spender,
			"nonce":        nonce + 1,
		},
	})
}

func (gc *GrapthwayClient) GetTokenAllowance(tokenAddress, owner, spender string) (map[string]interface{}, error) {
	return gc.request(fmt.Sprintf("/token/allowance?tokenAddress=%s&owner=%s&spender=%s", tokenAddress, owner, spender), &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

func (gc *GrapthwayClient) MintToken(tokenAddress, to string, amount float64) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/token/mint", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"tokenAddress": tokenAddress,
			"to":           to,
			"amount":       amount,
			"nonce":        nonce + 1,
		},
	})
}

func (gc *GrapthwayClient) BurnToken(tokenAddress string, amount float64) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/token/burn", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"tokenAddress": tokenAddress,
			"amount":       amount,
			"nonce":        nonce + 1,
		},
	})
}

func (gc *GrapthwayClient) SetTokenMintConfig(tokenAddress string, config map[string]interface{}) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	body := make(map[string]interface{})
	body["tokenAddress"] = tokenAddress
	body["nonce"] = nonce + 1
	for k, v := range config {
		body[k] = v
	}

	return gc.request("/token/mintable/set", &RequestOptions{
		Method: "POST",
		Body:   body,
	})
}

func (gc *GrapthwayClient) SetTokenBurnConfig(tokenAddress string, config map[string]interface{}) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	body := make(map[string]interface{})
	body["tokenAddress"] = tokenAddress
	body["nonce"] = nonce + 1
	for k, v := range config {
		body[k] = v
	}

	return gc.request("/token/burnable/set", &RequestOptions{
		Method: "POST",
		Body:   body,
	})
}

func (gc *GrapthwayClient) LockTokenConfig(tokenAddress string) (map[string]interface{}, error) {
	account, _, err := gc.getCurrentIdentity()
	if err != nil {
		return nil, err
	}

	nonce, err := gc.GetNonce(account.Address)
	if err != nil {
		return nil, err
	}

	return gc.request("/token/config/lock", &RequestOptions{
		Method: "POST",
		Body: map[string]interface{}{
			"tokenAddress": tokenAddress,
			"nonce":        nonce + 1,
		},
	})
}

type TokenHistoryOptions struct {
	TokenAddress string
	Page         int
	Limit        int
}

func (gc *GrapthwayClient) GetTokenHistory(address string, options *TokenHistoryOptions) (map[string]interface{}, error) {
	if options == nil {
		options = &TokenHistoryOptions{
			Page:  1,
			Limit: 10,
		}
	}

	query := fmt.Sprintf("address=%s&page=%d&limit=%d", address, options.Page, options.Limit)
	if options.TokenAddress != "" {
		query += fmt.Sprintf("&tokenAddress=%s", options.TokenAddress)
	}

	return gc.request(fmt.Sprintf("/token/history?%s", query), &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

// ========================================
// NODE & NETWORK INFO
// ========================================

func (gc *GrapthwayClient) GetGatewayStatus() (map[string]interface{}, error) {
	return gc.request("/gateway-status", &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

func (gc *GrapthwayClient) GetHardwareStats() (map[string]interface{}, error) {
	return gc.request("/hardware-stats", &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

func (gc *GrapthwayClient) GetServices() (map[string]interface{}, error) {
	return gc.request("/services", &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

func (gc *GrapthwayClient) GetPipelines() (map[string]interface{}, error) {
	return gc.request("/pipelines", &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

func (gc *GrapthwayClient) GetSchema() (map[string]interface{}, error) {
	return gc.request("/schema", &RequestOptions{
		AuthType: AuthTypePublic,
	})
}

// ========================================
// WORKFLOW OPERATIONS
// ========================================

func (gc *GrapthwayClient) GetWorkflowMonitoring(filters map[string]string) (map[string]interface{}, error) {
	query := url.Values{}
	for k, v := range filters {
		query.Add(k, v)
	}

	endpoint := "/workflows/monitoring"
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	return gc.request(endpoint, &RequestOptions{
		AuthType: AuthTypeUser,
	})
}

// ========================================
// LOGGING
// ========================================

type LogOptions struct {
	Start string
	End   string
	Max   int
}

func (gc *GrapthwayClient) GetLogs(logType string, options *LogOptions) (map[string]interface{}, error) {
	query := url.Values{}
	if options != nil {
		if options.Start != "" {
			query.Add("start", options.Start)
		}
		if options.End != "" {
			query.Add("end", options.End)
		}
		if options.Max > 0 {
			query.Add("max", strconv.Itoa(options.Max))
		}
	}

	endpoint := fmt.Sprintf("/logs/%s", logType)
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	return gc.request(endpoint, &RequestOptions{
		AuthType: AuthTypeUser,
	})
}

// ========================================
// ADMIN OPERATIONS
// ========================================

func (gc *GrapthwayClient) CreateWallet() (map[string]interface{}, error) {
	return gc.request("/wallet/create", &RequestOptions{
		Method:   "POST",
		AuthType: AuthTypeAdmin,
		Body:     map[string]interface{}{},
	})
}

func (gc *GrapthwayClient) GetAdminLogs(logType string, options *LogOptions) (map[string]interface{}, error) {
	query := url.Values{}
	if options != nil {
		if options.Start != "" {
			query.Add("start", options.Start)
		}
		if options.End != "" {
			query.Add("end", options.End)
		}
		if options.Max > 0 {
			query.Add("max", strconv.Itoa(options.Max))
		}
	}

	endpoint := fmt.Sprintf("/logs/%s", logType)
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	return gc.request(endpoint, &RequestOptions{
		AuthType: AuthTypeAdmin,
	})
}

func (gc *GrapthwayClient) GetAdminWorkflowMonitoring(filters map[string]string) (map[string]interface{}, error) {
	query := url.Values{}
	for k, v := range filters {
		query.Add(k, v)
	}

	endpoint := "/workflows/monitoring"
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}

	return gc.request(endpoint, &RequestOptions{
		AuthType: AuthTypeAdmin,
	})
}
