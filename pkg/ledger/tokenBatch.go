package ledger

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"grapthway/pkg/ledger/types"
	"log"
	"runtime"
	"sync"
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
	numWorkers := runtime.GOMAXPROCS(0) * 16
	if numWorkers == 0 {
		numWorkers = 8
	}

	errChan := make(chan error, 6)
	var wg sync.WaitGroup

	// Token creates — independent
	wg.Add(1)
	go func() {
		defer wg.Done()
		for tokenAddress, token := range batch.TokenCreates {
			tokenBytes, err := jsoniter.Marshal(token)
			if err != nil {
				errChan <- fmt.Errorf("failed to marshal token %s: %w", tokenAddress, err)
				return
			}
			if err := wb.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes); err != nil {
				errChan <- err
				return
			}
			if token.Metadata != "" {
				if err := wb.Set([]byte(types.TokenMetadataPrefix+tokenAddress), []byte(token.Metadata)); err != nil {
					errChan <- err
					return
				}
			}
		}
	}()

	// Token updates — independent
	wg.Add(1)
	go func() {
		defer wg.Done()
		for tokenAddress, token := range batch.TokenUpdates {
			tokenBytes, err := jsoniter.Marshal(token)
			if err != nil {
				errChan <- fmt.Errorf("failed to marshal updated token %s: %w", tokenAddress, err)
				return
			}
			if err := wb.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes); err != nil {
				errChan <- err
				return
			}
		}
	}()

	// Ownership — independent
	wg.Add(1)
	go func() {
		defer wg.Done()
		for tokenAddress, owners := range batch.OwnershipAdds {
			for owner := range owners {
				key := []byte(fmt.Sprintf("%s%s-%s", types.TokenOwnerPrefix, owner, tokenAddress))
				if err := wb.Set(key, []byte{1}); err != nil {
					errChan <- err
					return
				}
			}
		}
	}()

	// Absolute balances — independent (no DB reads needed)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for tokenAddress, ownerBalances := range batch.AbsoluteBalances {
			for owner, absoluteBalance := range ownerBalances {
				key := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, tokenAddress, owner))
				b := make([]byte, 8)
				binary.BigEndian.PutUint64(b, absoluteBalance)
				if err := wb.Set(key, b); err != nil {
					errChan <- err
					return
				}
			}
		}
	}()

	// Allowances + deletes — independent, parallel with own worker pool
	wg.Add(1)
	go func() {
		defer wg.Done()
		for tokenAddress, ownerAllowances := range batch.Allowances {
			for owner, spenderAllowances := range ownerAllowances {
				for spender, amount := range spenderAllowances {
					key := []byte(fmt.Sprintf("%s%s-%s-%s", types.TokenAllowancePrefix, tokenAddress, owner, spender))
					b := make([]byte, 8)
					binary.BigEndian.PutUint64(b, amount)
					if err := wb.Set(key, b); err != nil {
						errChan <- err
						return
					}
				}
			}
		}
		for tokenAddress, ownerDeletes := range batch.AllowanceDeletes {
			for owner, spenderDeletes := range ownerDeletes {
				for spender := range spenderDeletes {
					key := []byte(fmt.Sprintf("%s%s-%s-%s", types.TokenAllowancePrefix, tokenAddress, owner, spender))
					if err := wb.Delete(key); err != nil {
						log.Printf("WARN: Failed to delete allowance key %s: %v", key, err)
					}
				}
			}
		}
	}()

	// History entries — use worker pool since there can be many
	wg.Add(1)
	go func() {
		defer wg.Done()
		historyChan := make(chan types.TokenHistory, len(batch.HistoryEntries))
		for _, h := range batch.HistoryEntries {
			historyChan <- h
		}
		close(historyChan)

		var hwg sync.WaitGroup
		histWorkers := numWorkers
		if histWorkers > len(batch.HistoryEntries) {
			histWorkers = len(batch.HistoryEntries)
		}
		histErrChan := make(chan error, len(batch.HistoryEntries))
		for i := 0; i < histWorkers; i++ {
			hwg.Add(1)
			go func() {
				defer hwg.Done()
				for history := range historyChan {
					historyBytes, err := jsoniter.Marshal(history)
					if err != nil {
						histErrChan <- err
						return
					}
					ts := history.Timestamp.Format(time.RFC3339Nano)
					if history.From != "" && history.From != "0x0" {
						key := []byte(fmt.Sprintf("tidx-token-from-%s-%s-%s", history.From, ts, history.TxHash))
						if err := wb.Set(key, historyBytes); err != nil {
							histErrChan <- err
							return
						}
					}
					if history.To != "" && history.To != "0x0" {
						key := []byte(fmt.Sprintf("tidx-token-to-%s-%s-%s", history.To, ts, history.TxHash))
						if err := wb.Set(key, historyBytes); err != nil {
							histErrChan <- err
							return
						}
					}
				}
			}()
		}
		hwg.Wait()
		close(histErrChan)
		for err := range histErrChan {
			if err != nil {
				errChan <- err
				return
			}
		}
	}()

	// Wait for all six independent sections
	wg.Wait()
	close(errChan)

	// Drain errors from all six parallel sections before proceeding to delta phase
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	// Balance deltas must run AFTER absolute balances are written (dependency ordering).
	// Different token+owner pairs are independent — parallelized with a worker pool.
	// Same token+owner pairs within a batch are impossible by construction (map keys are unique).
	type balanceDeltaJob struct {
		tokenAddress string
		owner        string
		delta        int64
	}

	deltaJobs := make([]balanceDeltaJob, 0)
	for tokenAddress, ownerBalances := range batch.BalanceChanges {
		for owner, delta := range ownerBalances {
			deltaJobs = append(deltaJobs, balanceDeltaJob{
				tokenAddress: tokenAddress,
				owner:        owner,
				delta:        delta,
			})
		}
	}

	if len(deltaJobs) > 0 {
		numDeltaWorkers := runtime.GOMAXPROCS(0) * 16
		if numDeltaWorkers == 0 {
			numDeltaWorkers = 8
		}
		if numDeltaWorkers > len(deltaJobs) {
			numDeltaWorkers = len(deltaJobs)
		}

		jobChan := make(chan balanceDeltaJob, len(deltaJobs))
		for _, job := range deltaJobs {
			jobChan <- job
		}
		close(jobChan)

		deltaErrChan := make(chan error, len(deltaJobs))
		var deltaWg sync.WaitGroup
		for i := 0; i < numDeltaWorkers; i++ {
			deltaWg.Add(1)
			go func() {
				defer deltaWg.Done()
				for job := range jobChan {
					balanceKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, job.tokenAddress, job.owner))
					var currentBalance uint64

					// Check if we already set an absolute balance for this owner in this batch.
					// Reading batch.AbsoluteBalances is safe here because the parallel absolute-balance
					// write goroutine has already completed (wg.Wait() above).
					if batch.AbsoluteBalances[job.tokenAddress] != nil {
						if absBalance, exists := batch.AbsoluteBalances[job.tokenAddress][job.owner]; exists {
							currentBalance = absBalance

							var newBalance uint64
							if job.delta < 0 {
								if currentBalance < uint64(-job.delta) {
									deltaErrChan <- fmt.Errorf("insufficient token balance for %s (has %d, needs %d)", job.owner, currentBalance, uint64(-job.delta))
									return
								}
								newBalance = currentBalance - uint64(-job.delta)
							} else {
								newBalance = currentBalance + uint64(job.delta)
							}

							// Update the in-batch absolute balance so any subsequent read
							// of this entry within the same block sees the final value.
							// Safe: map keys are unique so no two workers touch the same entry.
							batch.AbsoluteBalances[job.tokenAddress][job.owner] = newBalance

							balanceBytes := make([]byte, 8)
							binary.BigEndian.PutUint64(balanceBytes, newBalance)
							if err := wb.Set(balanceKey, balanceBytes); err != nil {
								deltaErrChan <- err
								return
							}
							continue
						}
					}

					// No absolute balance in this batch — read current value from DB.
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
						deltaErrChan <- fmt.Errorf("failed to read current balance for %s-%s: %w", job.tokenAddress, job.owner, err)
						return
					}

					var newBalance uint64
					if job.delta < 0 {
						if currentBalance < uint64(-job.delta) {
							deltaErrChan <- fmt.Errorf("insufficient token balance for %s (has %d, needs %d)", job.owner, currentBalance, uint64(-job.delta))
							return
						}
						newBalance = currentBalance - uint64(-job.delta)
					} else {
						newBalance = currentBalance + uint64(job.delta)
					}

					balanceBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(balanceBytes, newBalance)
					if err := wb.Set(balanceKey, balanceBytes); err != nil {
						deltaErrChan <- err
						return
					}
				}
			}()
		}

		deltaWg.Wait()
		close(deltaErrChan)

		for err := range deltaErrChan {
			if err != nil {
				return err
			}
		}
	}

	log.Printf("[TOKEN-BATCH] Applied %d token creates, %d updates, %d balance changes, %d allowances, %d history entries",
		len(batch.TokenCreates), len(batch.TokenUpdates), len(batch.BalanceChanges),
		len(batch.Allowances), len(batch.HistoryEntries))
	return nil
}
