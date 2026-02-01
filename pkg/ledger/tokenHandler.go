package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"grapthway/pkg/ledger/types"

	jsoniter "github.com/json-iterator/go"
)

// Token transaction types for blockchain inclusion
const (
	TokenCreateTransaction     types.TransactionType = "TOKEN_CREATE"
	TokenTransferTransaction   types.TransactionType = "TOKEN_TRANSFER"
	TokenMintTransaction       types.TransactionType = "TOKEN_MINT"
	TokenBurnTransaction       types.TransactionType = "TOKEN_BURN"
	TokenApproveTransaction    types.TransactionType = "TOKEN_APPROVE"
	TokenRevokeTransaction     types.TransactionType = "TOKEN_REVOKE"
	TokenLockTransaction       types.TransactionType = "TOKEN_LOCK"
	TokenConfigBurnTransaction types.TransactionType = "TOKEN_CONFIG_BURN"
	TokenConfigMintTransaction types.TransactionType = "TOKEN_CONFIG_MINT"
)

// CreateTokenHandler handles token creation
func CreateTokenHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	creator, ok := r.Context().Value("authenticatedUser").(string)
	if !ok || creator == "" {
		http.Error(w, "Authenticated user not found", http.StatusInternalServerError)
		return
	}

	var req struct {
		TokenMetadata struct {
			Name          string          `json:"name"`
			Ticker        string          `json:"ticker"`
			Decimals      uint8           `json:"decimals"`
			InitialSupply float64         `json:"initialSupply"`
			TokenType     types.TokenType `json:"tokenType"`
			BurnConfig    struct {
				Enabled    bool `json:"enabled"`
				ManualBurn bool `json:"manualBurn"`
			} `json:"burnConfig"`
			MintConfig struct {
				Enabled    bool `json:"enabled"`
				ManualMint bool `json:"manualMint"`
			} `json:"mintConfig"`
			Metadata string `json:"metadata"`
		} `json:"tokenMetadata"`
		Nonce uint64 `json:"nonce"`
	}

	if err := jsoniter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Basic validation
	if req.TokenMetadata.Name == "" || req.TokenMetadata.Ticker == "" {
		http.Error(w, "Token name and ticker are required", http.StatusBadRequest)
		return
	}
	if req.TokenMetadata.Decimals > types.MaxDecimals {
		http.Error(w, fmt.Sprintf("Decimals cannot exceed %d", types.MaxDecimals), http.StatusBadRequest)
		return
	}
	if len(req.TokenMetadata.Metadata) > types.MaxMetadataSize {
		http.Error(w, fmt.Sprintf("Metadata size cannot exceed %d bytes", types.MaxMetadataSize), http.StatusBadRequest)
		return
	}

	multiplier := math.Pow10(int(req.TokenMetadata.Decimals))

	if req.TokenMetadata.InitialSupply < 0 {
		http.Error(w, "Initial supply cannot be negative", http.StatusBadRequest)
		return
	}

	if req.TokenMetadata.InitialSupply > float64(math.MaxUint64)/multiplier {
		http.Error(w, "Initial supply is too large", http.StatusBadRequest)
		return
	}

	supplyMicro := uint64(req.TokenMetadata.InitialSupply * multiplier)

	// Create a unique hash for the transaction ID
	ts := time.Now()
	idData := fmt.Sprintf("%s-%d-%d", creator, req.Nonce, ts.UnixNano())
	hash := sha256.Sum256([]byte(idData))
	txID := "tx-" + hex.EncodeToString(hash[:])

	// Create the transaction
	tokenMetadata := types.TokenMetadata{
		Name:          req.TokenMetadata.Name,
		Ticker:        req.TokenMetadata.Ticker,
		Decimals:      uint8(req.TokenMetadata.Decimals),
		InitialSupply: supplyMicro,
		TokenType:     string(req.TokenMetadata.TokenType),
		BurnConfig: types.BurnConfig{
			Enabled:    req.TokenMetadata.BurnConfig.Enabled,
			ManualBurn: req.TokenMetadata.BurnConfig.ManualBurn,
		},
		MintConfig: types.MintConfig{
			Enabled:    req.TokenMetadata.MintConfig.Enabled,
			ManualMint: req.TokenMetadata.MintConfig.ManualMint,
		},
		Metadata: req.TokenMetadata.Metadata,
	}

	tx := types.Transaction{
		ID:            txID,
		Type:          TokenCreateTransaction,
		From:          creator,
		Nonce:         req.Nonce,
		TokenMetadata: &tokenMetadata,
		Timestamp:     ts,
		CreatedAt:     ts,
	}

	processedTx, err := ledgerClient.ProcessTransactionSubmission(r.Context(), tx)
	if err != nil {
		http.Error(w, "Transaction failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(processedTx)
}

// GetOwnedTokensHandler retrieves all tokens owned by a user
func GetOwnedTokensHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	owner := r.URL.Query().Get("owner")
	if owner == "" {
		http.Error(w, "Missing 'owner' parameter", http.StatusBadRequest)
		return
	}

	tokenAddresses, err := ledgerClient.GetOwnedTokens(owner)
	if err != nil {
		http.Error(w, "Failed to get owned tokens: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Fetch full token details with balances
	var tokens []map[string]interface{}
	for _, addr := range tokenAddresses {
		token, err := ledgerClient.GetToken(addr)
		if err != nil {
			continue
		}

		balance, _ := ledgerClient.GetTokenBalance(addr, owner)

		tokens = append(tokens, map[string]interface{}{
			"token":   token,
			"balance": balance, // Already in micro units, frontend will convert
		})
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"owner":  owner,
		"tokens": tokens,
		"count":  len(tokens),
	})
}

// TransferTokenHandler handles transferring tokens
func TransferTokenHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	requester, ok := r.Context().Value("authenticatedUser").(string)
	if !ok || requester == "" {
		http.Error(w, "Authenticated user not found", http.StatusUnauthorized)
		return
	}

	var req struct {
		TokenAddress string  `json:"tokenAddress"`
		To           string  `json:"to"`
		Amount       float64 `json:"amount"`
		Nonce        uint64  `json:"nonce"`
	}
	if err := jsoniter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	token, err := ledgerClient.GetToken(req.TokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	amountMicro := uint64(req.Amount * math.Pow(10, float64(token.Decimals)))

	ts := time.Now()
	idData := fmt.Sprintf("%s-%s-%s-%d-%d", requester, req.To, req.TokenAddress, req.Nonce, ts.UnixNano())
	hash := sha256.Sum256([]byte(idData))
	txID := "tx-token-transfer-" + hex.EncodeToString(hash[:])

	tx := types.Transaction{
		ID:           txID,
		Type:         TokenTransferTransaction,
		From:         requester,
		To:           req.To,
		Amount:       amountMicro,
		Nonce:        req.Nonce,
		Timestamp:    ts,
		CreatedAt:    ts,
		TokenAddress: req.TokenAddress,
	}

	processedTx, err := ledgerClient.ProcessTransactionSubmission(r.Context(), tx)
	if err != nil {
		http.Error(w, "Transaction failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(processedTx)
}

// SetAllowanceHandler sets token allowance (approve)
func SetAllowanceHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	owner, ok := r.Context().Value("authenticatedUser").(string)
	if !ok || owner == "" {
		http.Error(w, "Authenticated user not found", http.StatusInternalServerError)
		return
	}

	var req struct {
		TokenAddress string  `json:"tokenAddress"`
		Spender      string  `json:"spender"`
		Amount       float64 `json:"amount"`
		Nonce        uint64  `json:"nonce"`
	}

	if err := jsoniter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	token, err := ledgerClient.GetToken(req.TokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	amountMicro := uint64(req.Amount * math.Pow(10, float64(token.Decimals)))

	ts := time.Now()
	idData := fmt.Sprintf("%s-%s-%s-%d-%d", owner, req.Spender, req.TokenAddress, req.Nonce, ts.UnixNano())
	hash := sha256.Sum256([]byte(idData))
	txID := "tx-token-approve-" + hex.EncodeToString(hash[:])

	// Create blockchain transaction
	tx := types.Transaction{
		ID:             txID,
		Type:           TokenApproveTransaction,
		From:           owner,
		To:             req.Spender,
		Amount:         amountMicro,
		Nonce:          req.Nonce,
		Timestamp:      ts,
		CreatedAt:      ts,
		TokenAddress:   req.TokenAddress,
		AllowanceLimit: amountMicro, // Store for consistency
	}

	processedTx, err := ledgerClient.ProcessTransactionSubmission(r.Context(), tx)
	if err != nil {
		http.Error(w, "Transaction failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"message":       "Allowance set successfully",
		"transactionId": processedTx.ID,
	})
}

// DeleteAllowanceHandler removes token allowance
func DeleteAllowanceHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	owner, ok := r.Context().Value("authenticatedUser").(string)
	if !ok || owner == "" {
		http.Error(w, "Authenticated user not found", http.StatusInternalServerError)
		return
	}

	var req struct {
		TokenAddress string `json:"tokenAddress"`
		Spender      string `json:"spender"`
		Nonce        uint64 `json:"nonce"`
	}

	if err := jsoniter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Verify token exists
	_, err := ledgerClient.GetToken(req.TokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	ts := time.Now()
	idData := fmt.Sprintf("%s-%s-%s-%d-%d", owner, req.Spender, req.TokenAddress, req.Nonce, ts.UnixNano())
	hash := sha256.Sum256([]byte(idData))
	txID := "tx-token-revoke-" + hex.EncodeToString(hash[:])

	// Create blockchain transaction
	tx := types.Transaction{
		ID:           txID,
		Type:         TokenRevokeTransaction,
		From:         owner,
		To:           req.Spender,
		Amount:       0, // Revoking is setting allowance to 0
		Nonce:        req.Nonce,
		Timestamp:    ts,
		CreatedAt:    ts,
		TokenAddress: req.TokenAddress,
	}

	processedTx, err := ledgerClient.ProcessTransactionSubmission(r.Context(), tx)
	if err != nil {
		http.Error(w, "Transaction failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"message":       "Allowance removed successfully",
		"transactionId": processedTx.ID,
	})
}

// GetAllowanceHandler retrieves token allowance
func GetAllowanceHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	tokenAddress := r.URL.Query().Get("tokenAddress")
	owner := r.URL.Query().Get("owner")
	spender := r.URL.Query().Get("spender")

	if tokenAddress == "" || owner == "" || spender == "" {
		http.Error(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	token, err := ledgerClient.GetToken(tokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	allowanceMicro, err := ledgerClient.GetTokenAllowance(tokenAddress, owner, spender)
	if err != nil {
		http.Error(w, "Failed to get allowance: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return raw micro units - frontend will handle conversion
	allowance := float64(allowanceMicro) / math.Pow(10, float64(token.Decimals))

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"tokenAddress": tokenAddress,
		"owner":        owner,
		"spender":      spender,
		"allowance":    allowance,
	})
}

// SetBurnableHandler updates burn configuration
func SetBurnableHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	requester, ok := r.Context().Value("authenticatedUser").(string)
	if !ok || requester == "" {
		http.Error(w, "Authenticated user not found", http.StatusInternalServerError)
		return
	}

	var req struct {
		TokenAddress string  `json:"tokenAddress"`
		Enabled      bool    `json:"enabled"`
		Rate         float64 `json:"rate"`
		ManualBurn   bool    `json:"manualBurn"`
		Nonce        uint64  `json:"nonce"`
	}

	if err := jsoniter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	token, err := ledgerClient.GetToken(req.TokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	// Only creator can update config
	if token.Creator != requester {
		http.Error(w, "Only token creator can update burn config", http.StatusForbidden)
		return
	}

	ts := time.Now()
	idData := fmt.Sprintf("%s-%s-burn-config-%d-%d", requester, req.TokenAddress, req.Nonce, ts.UnixNano())
	hash := sha256.Sum256([]byte(idData))
	txID := "tx-token-config-burn-" + hex.EncodeToString(hash[:])

	// token metadata transaction
	tokenMetadata := types.TokenMetadata{
		Name:          token.Name,
		Ticker:        token.Ticker,
		Decimals:      token.Decimals,
		InitialSupply: token.InitialSupply,
		TokenType:     string(token.TokenType),
		BurnConfig: types.BurnConfig{
			Enabled:       req.Enabled,
			BurnRatePerTx: req.Rate,
			ManualBurn:    req.ManualBurn,
		},
		MintConfig: types.MintConfig{
			Enabled:       token.MintConfig.Enabled,
			MintRatePerTx: token.MintConfig.MintRatePerTx,
			ManualMint:    token.MintConfig.ManualMint,
		},
		Metadata: token.Metadata,
	}

	// Create transaction to record this change on the blockchain
	tx := types.Transaction{
		ID:            txID,
		Type:          types.TransactionType("TOKEN_CONFIG_BURN"),
		From:          requester,
		To:            req.TokenAddress,
		Nonce:         req.Nonce,
		Timestamp:     ts,
		CreatedAt:     ts,
		TokenAddress:  req.TokenAddress,
		TokenMetadata: &tokenMetadata,
	}

	processedTx, err := ledgerClient.ProcessTransactionSubmission(r.Context(), tx)
	if err != nil {
		http.Error(w, "Transaction failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"message":       "Burn configuration updated successfully",
		"transactionId": processedTx.ID,
	})
}

// SetMintableHandler updates mint configuration
func SetMintableHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	requester, ok := r.Context().Value("authenticatedUser").(string)
	if !ok || requester == "" {
		http.Error(w, "Authenticated user not found", http.StatusInternalServerError)
		return
	}

	var req struct {
		TokenAddress string  `json:"tokenAddress"`
		Enabled      bool    `json:"enabled"`
		Rate         float64 `json:"rate"`
		ManualMint   bool    `json:"manualMint"`
		Nonce        uint64  `json:"nonce"`
	}

	if err := jsoniter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	token, err := ledgerClient.GetToken(req.TokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	// Only creator can update config
	if token.Creator != requester {
		http.Error(w, "Only token creator can update mint config", http.StatusForbidden)
		return
	}

	ts := time.Now()
	idData := fmt.Sprintf("%s-%s-mint-config-%d-%d", requester, req.TokenAddress, req.Nonce, ts.UnixNano())
	hash := sha256.Sum256([]byte(idData))
	txID := "tx-token-config-mint-" + hex.EncodeToString(hash[:])

	// token metadata transaction
	tokenMetadata := types.TokenMetadata{
		Name:          token.Name,
		Ticker:        token.Ticker,
		Decimals:      token.Decimals,
		InitialSupply: token.InitialSupply,
		TokenType:     string(token.TokenType),
		BurnConfig: types.BurnConfig{
			Enabled:       token.BurnConfig.Enabled,
			BurnRatePerTx: token.BurnConfig.BurnRatePerTx,
			ManualBurn:    token.BurnConfig.ManualBurn,
		},
		MintConfig: types.MintConfig{
			Enabled:       req.Enabled,
			MintRatePerTx: req.Rate,
			ManualMint:    req.ManualMint,
		},
		Metadata: token.Metadata,
	}

	// Create transaction to record this change
	tx := types.Transaction{
		ID:            txID,
		Type:          types.TransactionType("TOKEN_CONFIG_MINT"),
		From:          requester,
		To:            req.TokenAddress,
		Nonce:         req.Nonce,
		Timestamp:     ts,
		CreatedAt:     ts,
		TokenAddress:  req.TokenAddress,
		TokenMetadata: &tokenMetadata,
	}

	processedTx, err := ledgerClient.ProcessTransactionSubmission(r.Context(), tx)
	if err != nil {
		http.Error(w, "Transaction failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"message":       "Mint configuration updated successfully",
		"transactionId": processedTx.ID,
	})
}

// LockBurnMintHandler locks burn/mint configuration permanently
func LockBurnMintHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	requester, ok := r.Context().Value("authenticatedUser").(string)
	if !ok || requester == "" {
		http.Error(w, "Authenticated user not found", http.StatusInternalServerError)
		return
	}

	var req struct {
		TokenAddress string `json:"tokenAddress"`
		Nonce        uint64 `json:"nonce"`
	}

	if err := jsoniter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	token, err := ledgerClient.GetToken(req.TokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	// Only creator can lock config
	if token.Creator != requester {
		http.Error(w, "Only token creator can lock config", http.StatusForbidden)
		return
	}

	ts := time.Now()
	idData := fmt.Sprintf("%s-%s-%d-%d", requester, req.TokenAddress, req.Nonce, ts.UnixNano())
	hash := sha256.Sum256([]byte(idData))
	txID := "tx-token-lock-" + hex.EncodeToString(hash[:])

	// Create blockchain transaction
	tx := types.Transaction{
		ID:           txID,
		Type:         TokenLockTransaction,
		From:         requester,
		Nonce:        req.Nonce,
		Timestamp:    ts,
		CreatedAt:    ts,
		TokenAddress: req.TokenAddress,
	}

	processedTx, err := ledgerClient.ProcessTransactionSubmission(r.Context(), tx)
	if err != nil {
		http.Error(w, "Transaction failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"message":       "Configuration locked permanently",
		"transactionId": processedTx.ID,
	})
}

// ManualBurnHandler handles manual token burning
func ManualBurnHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	from, ok := r.Context().Value("authenticatedUser").(string)
	if !ok || from == "" {
		http.Error(w, "Authenticated user not found", http.StatusInternalServerError)
		return
	}

	var req struct {
		TokenAddress string  `json:"tokenAddress"`
		Amount       float64 `json:"amount"`
		Nonce        uint64  `json:"nonce"`
	}

	if err := jsoniter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	token, err := ledgerClient.GetToken(req.TokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	amountMicro := uint64(req.Amount * math.Pow(10, float64(token.Decimals)))

	ts := time.Now()
	idData := fmt.Sprintf("%s-%f-%s-%d-%d", from, req.Amount, req.TokenAddress, req.Nonce, ts.UnixNano())
	hash := sha256.Sum256([]byte(idData))
	txID := "tx-token-burn-" + hex.EncodeToString(hash[:])

	// Create blockchain transaction
	tx := types.Transaction{
		ID:           txID,
		Type:         TokenBurnTransaction,
		From:         from,
		Amount:       amountMicro,
		Nonce:        req.Nonce,
		Timestamp:    ts,
		CreatedAt:    ts,
		TokenAddress: req.TokenAddress,
	}

	processedTx, err := ledgerClient.ProcessTransactionSubmission(r.Context(), tx)
	if err != nil {
		http.Error(w, "Transaction failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"message":       "Tokens burned successfully",
		"transactionId": processedTx.ID,
	})
}

// ManualMintHandler handles manual token minting
func ManualMintHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	requester, ok := r.Context().Value("authenticatedUser").(string)
	if !ok || requester == "" {
		http.Error(w, "Authenticated user not found", http.StatusInternalServerError)
		return
	}

	var req struct {
		TokenAddress string  `json:"tokenAddress"`
		To           string  `json:"to"`
		Amount       float64 `json:"amount"`
		Nonce        uint64  `json:"nonce"`
	}

	if err := jsoniter.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	token, err := ledgerClient.GetToken(req.TokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	// Only creator can mint
	if token.Creator != requester {
		http.Error(w, "Only token creator can mint", http.StatusForbidden)
		return
	}

	amountMicro := uint64(req.Amount * math.Pow(10, float64(token.Decimals)))

	ts := time.Now()
	idData := fmt.Sprintf("%s-%f-%s-%d-%d", requester, req.Amount, req.TokenAddress, req.Nonce, ts.UnixNano())
	hash := sha256.Sum256([]byte(idData))
	txID := "tx-token-mint-" + hex.EncodeToString(hash[:])

	// Create blockchain transaction
	tx := types.Transaction{
		ID:           txID,
		Type:         TokenMintTransaction,
		From:         requester,
		To:           req.To,
		Amount:       amountMicro,
		Nonce:        req.Nonce,
		Timestamp:    ts,
		CreatedAt:    ts,
		TokenAddress: token.Address,
	}

	processedTx, err := ledgerClient.ProcessTransactionSubmission(r.Context(), tx)
	if err != nil {
		http.Error(w, "Transaction failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"message":       "Tokens minted successfully",
		"transactionId": processedTx.ID,
	})
}

// GetTokenHistoryHandler retrieves token transaction history for an address
func GetTokenHistoryHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	address := r.URL.Query().Get("address")
	if address == "" {
		http.Error(w, "Missing 'address' parameter", http.StatusBadRequest)
		return
	}

	tokenAddress := r.URL.Query().Get("tokenAddress") // Optional filter

	pageStr := r.URL.Query().Get("page")
	limitStr := r.URL.Query().Get("limit")

	page := 1
	if pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}

	limit := 10
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	history, err := ledgerClient.GetTokenHistory(address, tokenAddress, page, limit)
	if err != nil {
		http.Error(w, "Failed to get token history: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Enrich history with token metadata
	type EnrichedHistory struct {
		TokenAddress string               `json:"tokenAddress"`
		TokenName    string               `json:"tokenName"`
		TokenTicker  string               `json:"tokenTicker"`
		Operation    types.TokenOperation `json:"operation"`
		From         string               `json:"from"`
		To           string               `json:"to"`
		Amount       float64              `json:"amount"`
		AmountRaw    uint64               `json:"amountRaw"`
		Timestamp    time.Time            `json:"timestamp"`
		TxHash       string               `json:"txHash"`
	}

	enriched := make([]EnrichedHistory, len(history))
	for i, h := range history {
		token, err := ledgerClient.GetToken(h.TokenAddress)

		var tokenName, tokenTicker string
		var decimals uint8 = 0

		if err == nil && token != nil {
			tokenName = token.Name
			tokenTicker = token.Ticker
			decimals = token.Decimals
		}

		// Convert amount to human-readable format
		var amountFloat float64
		if decimals > 0 {
			amountFloat = float64(h.Amount) / math.Pow(10, float64(decimals))
		} else {
			amountFloat = float64(h.Amount)
		}

		enriched[i] = EnrichedHistory{
			TokenAddress: h.TokenAddress,
			TokenName:    tokenName,
			TokenTicker:  tokenTicker,
			Operation:    h.Operation,
			From:         h.From,
			To:           h.To,
			Amount:       amountFloat,
			AmountRaw:    h.Amount,
			Timestamp:    h.Timestamp,
			TxHash:       h.TxHash,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"address": address,
		"history": enriched,
		"page":    page,
		"limit":   limit,
		"count":   len(enriched),
	})
}

// GetTokenHandler retrieves token details
func GetTokenHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	tokenAddress := r.URL.Query().Get("address")
	if tokenAddress == "" {
		http.Error(w, "Missing 'address' parameter", http.StatusBadRequest)
		return
	}

	token, err := ledgerClient.GetToken(tokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(token)
}

// GetTokenBalanceHandler - Correct decimal conversion
func GetTokenBalanceHandler(w http.ResponseWriter, r *http.Request, ledgerClient *Client) {
	tokenAddress := r.URL.Query().Get("tokenAddress")
	owner := r.URL.Query().Get("owner")

	if tokenAddress == "" || owner == "" {
		http.Error(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	token, err := ledgerClient.GetToken(tokenAddress)
	if err != nil {
		http.Error(w, "Token not found: "+err.Error(), http.StatusNotFound)
		return
	}

	balanceMicro, err := ledgerClient.GetTokenBalance(tokenAddress, owner)
	if err != nil {
		http.Error(w, "Failed to get balance: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Use proper decimal conversion
	var balance float64
	if token.Decimals > 0 {
		balance = float64(balanceMicro) / math.Pow(10, float64(token.Decimals))
	} else {
		balance = float64(balanceMicro)
	}

	w.Header().Set("Content-Type", "application/json")
	jsoniter.NewEncoder(w).Encode(map[string]interface{}{
		"tokenAddress": tokenAddress,
		"tokenName":    token.Name,
		"tokenTicker":  token.Ticker,
		"owner":        owner,
		"balance":      balance,
		"balanceRaw":   balanceMicro,
		"decimals":     token.Decimals,
	})
}
