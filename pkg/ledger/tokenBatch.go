package ledger

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"grapthway/pkg/ledger/types"
	"log"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/dgraph-io/badger/v4"
)

// generateTokenAddressDeterministic creates a deterministic token address
// using the transaction hash instead of timestamp
func generateTokenAddressDeterministic(creator, name, ticker, txHash string) string {
	data := fmt.Sprintf("%s:%s:%s:%s", creator, name, ticker, txHash)
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("0xTKN%x", hash[:18])
}

// TokenBatchOperations holds pending token operations to be applied in a batch
type TokenBatchOperations struct {
	TokenCreates     map[string]*types.Token                 // tokenAddress -> token
	TokenUpdates     map[string]*types.Token                 // tokenAddress -> updated token
	BalanceChanges   map[string]map[string]int64             // tokenAddress -> ownerAddress -> delta
	AbsoluteBalances map[string]map[string]uint64            // tokenAddress -> ownerAddress -> absolute balance (for new tokens)
	Allowances       map[string]map[string]map[string]uint64 // tokenAddress -> owner -> spender -> amount
	AllowanceDeletes map[string]map[string]map[string]bool   // tokenAddress -> owner -> spender -> delete flag
	OwnershipAdds    map[string]map[string]bool              // tokenAddress -> ownerAddress -> true
	HistoryEntries   []types.TokenHistory                    // All history entries to write
}

// NewTokenBatchOperations creates a new batch operations tracker
func NewTokenBatchOperations() *TokenBatchOperations {
	return &TokenBatchOperations{
		TokenCreates:     make(map[string]*types.Token),
		TokenUpdates:     make(map[string]*types.Token),
		BalanceChanges:   make(map[string]map[string]int64),
		AbsoluteBalances: make(map[string]map[string]uint64),
		Allowances:       make(map[string]map[string]map[string]uint64),
		AllowanceDeletes: make(map[string]map[string]map[string]bool),
		OwnershipAdds:    make(map[string]map[string]bool),
		HistoryEntries:   make([]types.TokenHistory, 0),
	}
}

// CreateTokenInBatch records a token creation to be applied in batch
func (c *Client) CreateTokenInBatch(
	batch *TokenBatchOperations,
	txHash string,
	creator string,
	name string,
	ticker string,
	decimals uint8,
	initialSupply uint64,
	tokenType types.TokenType,
	burnConfig types.BurnConfig,
	mintConfig types.MintConfig,
	metadata string,
) (*types.Token, error) {

	// Validate inputs
	if creator == "" {
		return nil, fmt.Errorf("creator address is required")
	}
	if name == "" || ticker == "" {
		return nil, fmt.Errorf("name and ticker are required")
	}
	if decimals > types.MaxDecimals {
		return nil, fmt.Errorf("decimals cannot exceed %d", types.MaxDecimals)
	}
	if len(metadata) > types.MaxMetadataSize {
		return nil, fmt.Errorf("metadata size exceeds maximum of %d bytes", types.MaxMetadataSize)
	}

	switch tokenType {
	case types.FungibleToken, types.NonFungibleToken, types.RealWorldAsset:
		// Valid token type
	default:
		return nil, fmt.Errorf("invalid token type: %s. Must be FUNGIBLE, NFT, or RWA", tokenType)
	}

	if burnConfig.BurnRatePerTx < 0 || burnConfig.BurnRatePerTx > 100 {
		return nil, fmt.Errorf("burn rate must be between 0 and 100")
	}
	if mintConfig.MintRatePerTx < 0 || mintConfig.MintRatePerTx > 100 {
		return nil, fmt.Errorf("mint rate must be between 0 and 100")
	}

	// Generate deterministic token address using transaction hash (not timestamp!)
	tokenAddress := generateTokenAddressDeterministic(creator, name, ticker, txHash)

	token := &types.Token{
		Address:       tokenAddress,
		Name:          name,
		Ticker:        ticker,
		Decimals:      decimals,
		InitialSupply: initialSupply,
		TotalSupply:   initialSupply,
		Creator:       creator,
		CreatedAt:     time.Now(),
		TokenType:     tokenType,
		BurnConfig:    burnConfig,
		MintConfig:    mintConfig,
		ConfigLocked:  false,
		Metadata:      metadata,
	}

	// Record token creation in batch
	batch.TokenCreates[tokenAddress] = token

	// Record ownership
	if batch.OwnershipAdds[tokenAddress] == nil {
		batch.OwnershipAdds[tokenAddress] = make(map[string]bool)
	}
	batch.OwnershipAdds[tokenAddress][creator] = true

	// Record initial balance as ABSOLUTE balance (not delta)
	if initialSupply > 0 {
		if batch.AbsoluteBalances[tokenAddress] == nil {
			batch.AbsoluteBalances[tokenAddress] = make(map[string]uint64)
		}
		batch.AbsoluteBalances[tokenAddress][creator] = initialSupply
	}

	// Record history
	history := types.TokenHistory{
		TokenAddress: tokenAddress,
		Operation:    types.OpCreate,
		From:         "0x0",
		To:           creator,
		Amount:       initialSupply,
		Timestamp:    time.Now(),
		TxHash:       txHash,
	}
	batch.HistoryEntries = append(batch.HistoryEntries, history)

	// Update cache immediately so subsequent operations in same block can see it
	c.tokenCache.Set(tokenAddress, token)
	log.Printf("[TOKEN-CACHE] Cached newly created token %s in batch", tokenAddress)

	return token, nil
}

// TransferTokenInBatch records a token transfer to be applied in batch
func (c *Client) TransferTokenInBatch(
	batch *TokenBatchOperations,
	tokenAddress, from, to string,
	amount uint64,
	txHash string,
) error {
	if from == to {
		return fmt.Errorf("cannot transfer to self")
	}
	if amount == 0 {
		return fmt.Errorf("amount must be greater than zero")
	}

	// Get token from batch or cache/DB
	token, err := c.getTokenForBatch(batch, tokenAddress)
	if err != nil {
		return err
	}

	// Calculate burn and mint amounts
	burnAmount := uint64(0)
	if token.BurnConfig.Enabled && token.BurnConfig.BurnRatePerTx > 0 {
		burnAmount = uint64(float64(amount) * token.BurnConfig.BurnRatePerTx / 100.0)
	}

	mintAmount := uint64(0)
	if token.MintConfig.Enabled && token.MintConfig.MintRatePerTx > 0 {
		mintAmount = uint64(float64(amount) * token.MintConfig.MintRatePerTx / 100.0)
	}

	actualTransferAmount := amount - burnAmount

	// Check if sender has an absolute balance (newly created token)
	// If so, deduct from absolute balance directly instead of using deltas
	if batch.AbsoluteBalances[tokenAddress] != nil {
		if absBalance, exists := batch.AbsoluteBalances[tokenAddress][from]; exists {
			// Sender has absolute balance, update it directly
			newFromBalance := absBalance - amount
			batch.AbsoluteBalances[tokenAddress][from] = newFromBalance

			// For recipient, check if they also have absolute balance
			if absToBalance, toExists := batch.AbsoluteBalances[tokenAddress][to]; toExists {
				batch.AbsoluteBalances[tokenAddress][to] = absToBalance + actualTransferAmount + mintAmount
			} else {
				// Recipient doesn't have absolute balance, use delta
				if batch.BalanceChanges[tokenAddress] == nil {
					batch.BalanceChanges[tokenAddress] = make(map[string]int64)
				}
				batch.BalanceChanges[tokenAddress][to] += int64(actualTransferAmount + mintAmount)
			}
		} else {
			// Sender doesn't have absolute balance, use normal deltas
			if batch.BalanceChanges[tokenAddress] == nil {
				batch.BalanceChanges[tokenAddress] = make(map[string]int64)
			}
			batch.BalanceChanges[tokenAddress][from] -= int64(amount)
			batch.BalanceChanges[tokenAddress][to] += int64(actualTransferAmount + mintAmount)
		}
	} else {
		// No absolute balances for this token, use normal deltas
		if batch.BalanceChanges[tokenAddress] == nil {
			batch.BalanceChanges[tokenAddress] = make(map[string]int64)
		}
		batch.BalanceChanges[tokenAddress][from] -= int64(amount)
		batch.BalanceChanges[tokenAddress][to] += int64(actualTransferAmount + mintAmount)
	}

	// Update total supply if needed
	if burnAmount > 0 || mintAmount > 0 {
		updatedToken := *token
		updatedToken.TotalSupply = updatedToken.TotalSupply - burnAmount + mintAmount
		batch.TokenUpdates[tokenAddress] = &updatedToken

		// Update cache
		c.tokenCache.Set(tokenAddress, &updatedToken)
	}

	// Record history
	history := types.TokenHistory{
		TokenAddress: tokenAddress,
		Operation:    types.OpTransfer,
		From:         from,
		To:           to,
		Amount:       amount,
		Timestamp:    time.Now(),
		TxHash:       txHash,
	}
	batch.HistoryEntries = append(batch.HistoryEntries, history)

	return nil
}

// ManualMintTokenInBatch records a token mint to be applied in batch
func (c *Client) ManualMintTokenInBatch(
	batch *TokenBatchOperations,
	tokenAddress, creator, to string,
	amount uint64,
	txHash string,
) error {
	token, err := c.getTokenForBatch(batch, tokenAddress)
	if err != nil {
		return err
	}

	if token.Creator != creator {
		return fmt.Errorf("only creator can mint")
	}
	if !token.MintConfig.Enabled || !token.MintConfig.ManualMint {
		return fmt.Errorf("manual mint not enabled")
	}

	// Record balance change
	if batch.BalanceChanges[tokenAddress] == nil {
		batch.BalanceChanges[tokenAddress] = make(map[string]int64)
	}
	batch.BalanceChanges[tokenAddress][to] += int64(amount)

	// Update total supply
	updatedToken := *token
	updatedToken.TotalSupply += amount
	batch.TokenUpdates[tokenAddress] = &updatedToken
	c.tokenCache.Set(tokenAddress, &updatedToken)

	// Record history
	history := types.TokenHistory{
		TokenAddress: tokenAddress,
		Operation:    types.OpMint,
		From:         "0x0",
		To:           to,
		Amount:       amount,
		Timestamp:    time.Now(),
		TxHash:       txHash,
	}
	batch.HistoryEntries = append(batch.HistoryEntries, history)

	return nil
}

// ManualBurnTokenInBatch records a token burn to be applied in batch
func (c *Client) ManualBurnTokenInBatch(
	batch *TokenBatchOperations,
	tokenAddress, from string,
	amount uint64,
	txHash string,
) error {
	token, err := c.getTokenForBatch(batch, tokenAddress)
	if err != nil {
		return err
	}

	if !token.BurnConfig.Enabled || !token.BurnConfig.ManualBurn {
		return fmt.Errorf("manual burn not enabled")
	}

	// Record balance change
	if batch.BalanceChanges[tokenAddress] == nil {
		batch.BalanceChanges[tokenAddress] = make(map[string]int64)
	}
	batch.BalanceChanges[tokenAddress][from] -= int64(amount)

	// Update total supply
	updatedToken := *token
	updatedToken.TotalSupply -= amount
	batch.TokenUpdates[tokenAddress] = &updatedToken
	c.tokenCache.Set(tokenAddress, &updatedToken)

	// Record history
	history := types.TokenHistory{
		TokenAddress: tokenAddress,
		Operation:    types.OpBurn,
		From:         from,
		To:           "0x0",
		Amount:       amount,
		Timestamp:    time.Now(),
		TxHash:       txHash,
	}
	batch.HistoryEntries = append(batch.HistoryEntries, history)

	return nil
}

// SetTokenAllowanceInBatch records a token allowance to be applied in batch
func (c *Client) SetTokenAllowanceInBatch(
	batch *TokenBatchOperations,
	tokenAddress, owner, spender string,
	amount uint64,
	txHash string,
) error {
	_, err := c.getTokenForBatch(batch, tokenAddress)
	if err != nil {
		return err
	}

	// Record allowance
	if batch.Allowances[tokenAddress] == nil {
		batch.Allowances[tokenAddress] = make(map[string]map[string]uint64)
	}
	if batch.Allowances[tokenAddress][owner] == nil {
		batch.Allowances[tokenAddress][owner] = make(map[string]uint64)
	}
	batch.Allowances[tokenAddress][owner][spender] = amount

	// Record history
	history := types.TokenHistory{
		TokenAddress: tokenAddress,
		Operation:    types.OpApprove,
		From:         owner,
		To:           spender,
		Amount:       amount,
		Timestamp:    time.Now(),
		TxHash:       txHash,
	}
	batch.HistoryEntries = append(batch.HistoryEntries, history)

	return nil
}

// DeleteTokenAllowanceInBatch records a token allowance deletion to be applied in batch
func (c *Client) DeleteTokenAllowanceInBatch(
	batch *TokenBatchOperations,
	tokenAddress, owner, spender string,
	txHash string,
) error {
	_, err := c.getTokenForBatch(batch, tokenAddress)
	if err != nil {
		return err
	}

	// Record allowance deletion
	if batch.AllowanceDeletes[tokenAddress] == nil {
		batch.AllowanceDeletes[tokenAddress] = make(map[string]map[string]bool)
	}
	if batch.AllowanceDeletes[tokenAddress][owner] == nil {
		batch.AllowanceDeletes[tokenAddress][owner] = make(map[string]bool)
	}
	batch.AllowanceDeletes[tokenAddress][owner][spender] = true

	return nil
}

// LockTokenConfigInBatch records a token config lock to be applied in batch
func (c *Client) LockTokenConfigInBatch(
	batch *TokenBatchOperations,
	tokenAddress, creator string,
	txHash string,
) error {
	token, err := c.getTokenForBatch(batch, tokenAddress)
	if err != nil {
		return err
	}

	if token.Creator != creator {
		return fmt.Errorf("only creator can lock config")
	}
	if token.ConfigLocked {
		return fmt.Errorf("config already locked")
	}

	// Update token
	updatedToken := *token
	updatedToken.ConfigLocked = true
	batch.TokenUpdates[tokenAddress] = &updatedToken
	c.tokenCache.Set(tokenAddress, &updatedToken)

	// Record history
	history := types.TokenHistory{
		TokenAddress: tokenAddress,
		Operation:    types.OpConfigLock,
		From:         creator,
		To:           tokenAddress,
		Amount:       0,
		Timestamp:    time.Now(),
		TxHash:       txHash,
	}
	batch.HistoryEntries = append(batch.HistoryEntries, history)

	return nil
}

// SetBurnableConfigInBatch records burn config changes to be applied in batch
func (c *Client) SetBurnableConfigInBatch(
	batch *TokenBatchOperations,
	tokenAddress, creator string,
	burnConfig types.BurnConfig,
	txHash string,
) error {
	token, err := c.getTokenForBatch(batch, tokenAddress)
	if err != nil {
		return err
	}

	if token.Creator != creator {
		return fmt.Errorf("only creator can modify config")
	}
	if token.ConfigLocked {
		return fmt.Errorf("config is locked")
	}

	// Update token
	updatedToken := *token
	updatedToken.BurnConfig = burnConfig
	batch.TokenUpdates[tokenAddress] = &updatedToken
	c.tokenCache.Set(tokenAddress, &updatedToken)

	// Record history
	history := types.TokenHistory{
		TokenAddress: tokenAddress,
		Operation:    types.OpConfigBurn,
		From:         creator,
		To:           tokenAddress,
		Amount:       0,
		Timestamp:    time.Now(),
		TxHash:       txHash,
	}
	batch.HistoryEntries = append(batch.HistoryEntries, history)

	return nil
}

// SetMintableConfigInBatch records mint config changes to be applied in batch
func (c *Client) SetMintableConfigInBatch(
	batch *TokenBatchOperations,
	tokenAddress, creator string,
	mintConfig types.MintConfig,
	txHash string,
) error {
	token, err := c.getTokenForBatch(batch, tokenAddress)
	if err != nil {
		return err
	}

	if token.Creator != creator {
		return fmt.Errorf("only creator can modify config")
	}
	if token.ConfigLocked {
		return fmt.Errorf("config is locked")
	}

	// Update token
	updatedToken := *token
	updatedToken.MintConfig = mintConfig
	batch.TokenUpdates[tokenAddress] = &updatedToken
	c.tokenCache.Set(tokenAddress, &updatedToken)

	// Record history
	history := types.TokenHistory{
		TokenAddress: tokenAddress,
		Operation:    types.OpConfigMint,
		From:         creator,
		To:           tokenAddress,
		Amount:       0,
		Timestamp:    time.Now(),
		TxHash:       txHash,
	}
	batch.HistoryEntries = append(batch.HistoryEntries, history)

	return nil
}

// getTokenForBatch retrieves a token from batch creates, batch updates, cache, or DB
func (c *Client) getTokenForBatch(batch *TokenBatchOperations, tokenAddress string) (*types.Token, error) {
	// Check batch creates first (tokens created in this block)
	if token, exists := batch.TokenCreates[tokenAddress]; exists {
		return token, nil
	}

	// Check batch updates (tokens modified in this block)
	if token, exists := batch.TokenUpdates[tokenAddress]; exists {
		return token, nil
	}

	// Fall back to cache/DB
	return c.GetTokenFromCacheOrDB(tokenAddress)
}

// ApplyTokenBatchToWriteBatch writes all token operations to the Badger write batch
func (c *Client) ApplyTokenBatchToWriteBatch(batch *TokenBatchOperations, wb *badger.WriteBatch) error {
	// Write new tokens
	for tokenAddress, token := range batch.TokenCreates {
		tokenBytes, err := jsoniter.Marshal(token)
		if err != nil {
			return fmt.Errorf("failed to marshal token %s: %w", tokenAddress, err)
		}
		if err := wb.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes); err != nil {
			return err
		}

		// Write metadata if present
		if token.Metadata != "" {
			metaKey := []byte(types.TokenMetadataPrefix + tokenAddress)
			if err := wb.Set(metaKey, []byte(token.Metadata)); err != nil {
				return err
			}
		}
	}

	// Write token updates
	for tokenAddress, token := range batch.TokenUpdates {
		tokenBytes, err := jsoniter.Marshal(token)
		if err != nil {
			return fmt.Errorf("failed to marshal updated token %s: %w", tokenAddress, err)
		}
		if err := wb.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes); err != nil {
			return err
		}
	}

	// Write ownership records
	for tokenAddress, owners := range batch.OwnershipAdds {
		for owner := range owners {
			ownerKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenOwnerPrefix, owner, tokenAddress))
			if err := wb.Set(ownerKey, []byte{1}); err != nil {
				return err
			}
		}
	}

	// First, write absolute balances (from token creation)
	for tokenAddress, ownerBalances := range batch.AbsoluteBalances {
		for owner, absoluteBalance := range ownerBalances {
			balanceKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, tokenAddress, owner))
			balanceBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(balanceBytes, absoluteBalance)
			if err := wb.Set(balanceKey, balanceBytes); err != nil {
				return err
			}
		}
	}

	// Then apply balance changes (deltas from transfers, mints, burns)
	for tokenAddress, ownerBalances := range batch.BalanceChanges {
		for owner, delta := range ownerBalances {
			balanceKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, tokenAddress, owner))

			var currentBalance uint64

			// Check if we already set an absolute balance for this owner in this batch
			if batch.AbsoluteBalances[tokenAddress] != nil {
				if absBalance, exists := batch.AbsoluteBalances[tokenAddress][owner]; exists {
					// Use the absolute balance we just set
					currentBalance = absBalance

					// Apply delta
					var newBalance uint64
					if delta < 0 {
						if currentBalance < uint64(-delta) {
							return fmt.Errorf("insufficient token balance for %s (has %d, needs %d)", owner, currentBalance, uint64(-delta))
						}
						newBalance = currentBalance - uint64(-delta)
					} else {
						newBalance = currentBalance + uint64(delta)
					}

					// Update the absolute balance map so subsequent operations see the new balance
					batch.AbsoluteBalances[tokenAddress][owner] = newBalance

					// Write new balance
					balanceBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(balanceBytes, newBalance)
					if err := wb.Set(balanceKey, balanceBytes); err != nil {
						return err
					}
					continue
				}
			}

			// No absolute balance set, need to read from DB
			err := c.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(balanceKey)
				if err == badger.ErrKeyNotFound {
					currentBalance = 0
					return nil
				}
				if err != nil {
					return err
				}
				return item.Value(func(val []byte) error {
					currentBalance = binary.BigEndian.Uint64(val)
					return nil
				})
			})
			if err != nil {
				return fmt.Errorf("failed to read current balance for %s-%s: %w", tokenAddress, owner, err)
			}

			// Apply delta
			var newBalance uint64
			if delta < 0 {
				if currentBalance < uint64(-delta) {
					return fmt.Errorf("insufficient token balance for %s (has %d, needs %d)", owner, currentBalance, uint64(-delta))
				}
				newBalance = currentBalance - uint64(-delta)
			} else {
				newBalance = currentBalance + uint64(delta)
			}

			// Write new balance
			balanceBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(balanceBytes, newBalance)
			if err := wb.Set(balanceKey, balanceBytes); err != nil {
				return err
			}
		}
	}

	// Write allowances
	for tokenAddress, ownerAllowances := range batch.Allowances {
		for owner, spenderAllowances := range ownerAllowances {
			for spender, amount := range spenderAllowances {
				allowanceKey := []byte(fmt.Sprintf("%s%s-%s-%s", types.TokenAllowancePrefix, tokenAddress, owner, spender))
				allowanceBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(allowanceBytes, amount)
				if err := wb.Set(allowanceKey, allowanceBytes); err != nil {
					return err
				}
			}
		}
	}

	// Delete allowances
	for tokenAddress, ownerDeletes := range batch.AllowanceDeletes {
		for owner, spenderDeletes := range ownerDeletes {
			for spender := range spenderDeletes {
				allowanceKey := []byte(fmt.Sprintf("%s%s-%s-%s", types.TokenAllowancePrefix, tokenAddress, owner, spender))
				if err := wb.Delete(allowanceKey); err != nil {
					log.Printf("WARN: Failed to delete allowance key %s: %v", allowanceKey, err)
				}
			}
		}
	}

	// Write history entries
	for _, history := range batch.HistoryEntries {
		historyBytes, err := jsoniter.Marshal(history)
		if err != nil {
			return fmt.Errorf("failed to marshal token history: %w", err)
		}

		timestampStr := history.Timestamp.Format(time.RFC3339Nano)

		// Index for sender's history
		if history.From != "" && history.From != "0x0" {
			fromHistoryKey := []byte(fmt.Sprintf("tidx-token-from-%s-%s-%s", history.From, timestampStr, history.TxHash))
			if err := wb.Set(fromHistoryKey, historyBytes); err != nil {
				return err
			}
		}

		// Index for recipient's history
		if history.To != "" && history.To != "0x0" {
			toHistoryKey := []byte(fmt.Sprintf("tidx-token-to-%s-%s-%s", history.To, timestampStr, history.TxHash))
			if err := wb.Set(toHistoryKey, historyBytes); err != nil {
				return err
			}
		}
	}

	log.Printf("[TOKEN-BATCH] Applied %d token creates, %d updates, %d balance changes, %d allowances, %d history entries",
		len(batch.TokenCreates), len(batch.TokenUpdates), len(batch.BalanceChanges),
		len(batch.Allowances), len(batch.HistoryEntries))

	return nil
}
