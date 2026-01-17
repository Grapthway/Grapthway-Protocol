package ledger

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"grapthway/pkg/ledger/types"
	"sort"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/dgraph-io/badger/v4"
)

// CreateToken creates a new token
func (c *Client) CreateToken(
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

	tokenAddress := c.generateTokenAddress(creator, name, ticker, time.Now())

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

	err := c.db.Update(func(txn *badger.Txn) error {
		tokenBytes, err := jsoniter.Marshal(token)
		if err != nil {
			return fmt.Errorf("failed to marshal token: %w", err)
		}
		if err := txn.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes); err != nil {
			return err
		}

		ownerKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenOwnerPrefix, creator, tokenAddress))
		if err := txn.Set(ownerKey, []byte{1}); err != nil {
			return err
		}

		if initialSupply > 0 {
			balanceKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, tokenAddress, creator))
			balanceBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(balanceBytes, initialSupply)
			if err := txn.Set(balanceKey, balanceBytes); err != nil {
				return err
			}
		}

		if metadata != "" {
			metaKey := []byte(types.TokenMetadataPrefix + tokenAddress)
			if err := txn.Set(metaKey, []byte(metadata)); err != nil {
				return err
			}
		}

		// FIXED: Record creation in history with PROPER INDEXING
		history := types.TokenHistory{
			TokenAddress: tokenAddress,
			Operation:    types.OpCreate,
			From:         "0x0",
			To:           creator,
			Amount:       initialSupply,
			Timestamp:    time.Now(),
			TxHash:       txHash,
		}
		historyBytes, _ := jsoniter.Marshal(history)

		// NEW: Write with proper indexes
		timestampStr := history.Timestamp.Format(time.RFC3339Nano)

		// Index for creator's history (as recipient)
		toHistoryKey := []byte(fmt.Sprintf("tidx-token-to-%s-%s-%s", creator, timestampStr, history.TxHash))
		if err := txn.Set(toHistoryKey, historyBytes); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create token: %w", err)
	}

	return token, nil
}

// GetToken retrieves a token by address
// In token.go GetToken function - verify error propagation
func (c *Client) GetToken(tokenAddress string) (*types.Token, error) {
	var token types.Token
	err := c.db.View(func(txn *badger.Txn) error {
		// CRITICAL: Check token exists first
		item, err := txn.Get([]byte(types.TokenPrefix + tokenAddress))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return jsoniter.Unmarshal(val, &token)
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("token not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}
	return &token, nil
}

// GetOwnedTokens retrieves all tokens owned by an address
func (c *Client) GetOwnedTokens(owner string) ([]string, error) {
	var tokenAddresses []string

	err := c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(types.TokenOwnerPrefix + owner + "-")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := string(it.Item().Key())
			// Extract token address from key: "token-owner-{owner}-{tokenAddress}"
			parts := splitKey(key, types.TokenOwnerPrefix+owner+"-")
			if len(parts) > 0 {
				tokenAddresses = append(tokenAddresses, parts[0])
			}
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get owned tokens: %w", err)
	}

	return tokenAddresses, nil
}

// GetTokenBalance retrieves the balance of a token for an owner
func (c *Client) GetTokenBalance(tokenAddress, owner string) (uint64, error) {
	var balance uint64

	err := c.db.View(func(txn *badger.Txn) error {
		// NEW: Verify token exists first
		if _, err := txn.Get([]byte(types.TokenPrefix + tokenAddress)); err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("token not found")
			}
			return err
		}

		key := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, tokenAddress, owner))
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			balance = 0
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("invalid balance data")
			}
			balance = binary.BigEndian.Uint64(val)
			return nil
		})
	})

	if err != nil {
		return 0, fmt.Errorf("failed to get token balance: %w", err)
	}

	return balance, nil
}

// TransferToken transfers tokens from one address to another
func (c *Client) TransferToken(tokenAddress, from, to string, amount uint64, txHash string) error {
	if from == to {
		return fmt.Errorf("cannot transfer to self")
	}
	if amount == 0 {
		return fmt.Errorf("amount must be greater than zero")
	}

	return c.db.Update(func(txn *badger.Txn) error {
		tokenItem, err := txn.Get([]byte(types.TokenPrefix + tokenAddress))
		if err != nil {
			return fmt.Errorf("token not found")
		}

		var token types.Token
		if err := tokenItem.Value(func(val []byte) error {
			return jsoniter.Unmarshal(val, &token)
		}); err != nil {
			return err
		}

		burnAmount := uint64(0)
		if token.BurnConfig.Enabled && token.BurnConfig.BurnRatePerTx > 0 {
			burnAmount = uint64(float64(amount) * token.BurnConfig.BurnRatePerTx / 100.0)
		}

		mintAmount := uint64(0)
		if token.MintConfig.Enabled && token.MintConfig.MintRatePerTx > 0 {
			mintAmount = uint64(float64(amount) * token.MintConfig.MintRatePerTx / 100.0)
		}

		actualTransferAmount := amount - burnAmount

		fromKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, tokenAddress, from))
		fromItem, err := txn.Get(fromKey)
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("insufficient balance")
		}
		if err != nil {
			return err
		}

		var fromBalance uint64
		if err := fromItem.Value(func(val []byte) error {
			fromBalance = binary.BigEndian.Uint64(val)
			return nil
		}); err != nil {
			return err
		}

		if fromBalance < amount {
			return fmt.Errorf("insufficient balance")
		}

		newFromBalance := fromBalance - amount
		fromBalanceBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(fromBalanceBytes, newFromBalance)
		if err := txn.Set(fromKey, fromBalanceBytes); err != nil {
			return err
		}

		toKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, tokenAddress, to))
		var toBalance uint64
		toItem, err := txn.Get(toKey)
		if err == nil {
			toItem.Value(func(val []byte) error {
				toBalance = binary.BigEndian.Uint64(val)
				return nil
			})
		}

		newToBalance := toBalance + actualTransferAmount + mintAmount
		toBalanceBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(toBalanceBytes, newToBalance)
		if err := txn.Set(toKey, toBalanceBytes); err != nil {
			return err
		}

		if burnAmount > 0 || mintAmount > 0 {
			token.TotalSupply = token.TotalSupply - burnAmount + mintAmount
			tokenBytes, _ := jsoniter.Marshal(&token)
			if err := txn.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes); err != nil {
				return err
			}
		}

		ownerKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenOwnerPrefix, to, tokenAddress))
		if _, err := txn.Get(ownerKey); err == badger.ErrKeyNotFound {
			txn.Set(ownerKey, []byte{1})
		}

		// FIXED: Record in history with PROPER INDEXING
		history := types.TokenHistory{
			TokenAddress: tokenAddress,
			Operation:    types.OpTransfer,
			From:         from,
			To:           to,
			Amount:       amount,
			Timestamp:    time.Now(),
			TxHash:       txHash,
		}
		historyBytes, _ := jsoniter.Marshal(history)

		// NEW: Write with proper indexes
		timestampStr := history.Timestamp.Format(time.RFC3339Nano)

		// Index for sender's history
		fromHistoryKey := []byte(fmt.Sprintf("tidx-token-from-%s-%s-%s", from, timestampStr, txHash))
		if err := txn.Set(fromHistoryKey, historyBytes); err != nil {
			return err
		}

		// Index for receiver's history
		toHistoryKey := []byte(fmt.Sprintf("tidx-token-to-%s-%s-%s", to, timestampStr, txHash))
		if err := txn.Set(toHistoryKey, historyBytes); err != nil {
			return err
		}

		return nil
	})
}

// SetTokenAllowance sets allowance for a spender (like ERC20 approve)
func (c *Client) SetTokenAllowance(tokenAddress, owner, spender string, amount uint64, txHash string) error {
	if owner == spender {
		return fmt.Errorf("cannot approve self")
	}

	return c.db.Update(func(txn *badger.Txn) error {
		// Verify token exists and get token info for history
		tokenItem, err := txn.Get([]byte(types.TokenPrefix + tokenAddress))
		if err != nil {
			return fmt.Errorf("token not found")
		}

		var token types.Token
		if err := tokenItem.Value(func(val []byte) error {
			return jsoniter.Unmarshal(val, &token)
		}); err != nil {
			return err
		}

		// Store allowance
		allowanceKey := []byte(fmt.Sprintf("%s%s-%s-%s", types.TokenAllowancePrefix,
			tokenAddress, owner, spender))
		allowanceBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(allowanceBytes, amount)

		if err := txn.Set(allowanceKey, allowanceBytes); err != nil {
			return err
		}

		// NEW: Record allowance change in history with token metadata
		history := types.TokenHistory{
			TokenAddress: tokenAddress,
			Operation:    types.OpApprove,
			From:         owner,
			To:           spender,
			Amount:       amount,
			Timestamp:    time.Now(),
			TxHash:       txHash,
		}
		historyBytes, _ := jsoniter.Marshal(history)

		timestampStr := history.Timestamp.Format(time.RFC3339Nano)

		// Index for owner's history
		ownerHistoryKey := []byte(fmt.Sprintf("tidx-token-from-%s-%s-%s", owner, timestampStr, history.TxHash))
		if err := txn.Set(ownerHistoryKey, historyBytes); err != nil {
			return err
		}

		// Index for spender's history
		spenderHistoryKey := []byte(fmt.Sprintf("tidx-token-to-%s-%s-%s", spender, timestampStr, history.TxHash))
		if err := txn.Set(spenderHistoryKey, historyBytes); err != nil {
			return err
		}

		return nil
	})
}

// GetTokenAllowance retrieves allowance amount
func (c *Client) GetTokenAllowance(tokenAddress, owner, spender string) (uint64, error) {
	var allowance uint64

	err := c.db.View(func(txn *badger.Txn) error {
		// NEW: Verify token exists first
		if _, err := txn.Get([]byte(types.TokenPrefix + tokenAddress)); err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("token not found")
			}
			return err
		}

		key := []byte(fmt.Sprintf("%s%s-%s-%s", types.TokenAllowancePrefix,
			tokenAddress, owner, spender))
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			allowance = 0
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("invalid allowance data")
			}
			allowance = binary.BigEndian.Uint64(val)
			return nil
		})
	})

	if err != nil {
		return 0, fmt.Errorf("failed to get allowance: %w", err)
	}

	return allowance, nil
}

// DeleteTokenAllowance removes an allowance
func (c *Client) DeleteTokenAllowance(tokenAddress, owner, spender string, txHash string) error {
	return c.db.Update(func(txn *badger.Txn) error {
		// Get token info for history
		tokenItem, err := txn.Get([]byte(types.TokenPrefix + tokenAddress))
		if err != nil {
			return fmt.Errorf("token not found")
		}

		var token types.Token
		if err := tokenItem.Value(func(val []byte) error {
			return jsoniter.Unmarshal(val, &token)
		}); err != nil {
			return err
		}

		key := []byte(fmt.Sprintf("%s%s-%s-%s", types.TokenAllowancePrefix,
			tokenAddress, owner, spender))

		if err := txn.Delete(key); err != nil {
			return err
		}

		// NEW: Record allowance removal in history
		history := types.TokenHistory{
			TokenAddress: tokenAddress,
			Operation:    types.OpApprove, // Use same operation type with amount 0
			From:         owner,
			To:           spender,
			Amount:       0, // Zero amount indicates revocation
			Timestamp:    time.Now(),
			TxHash:       txHash,
		}
		historyBytes, _ := jsoniter.Marshal(history)

		timestampStr := history.Timestamp.Format(time.RFC3339Nano)

		// Index for owner's history
		ownerHistoryKey := []byte(fmt.Sprintf("tidx-token-from-%s-%s-%s", owner, timestampStr, history.TxHash))
		if err := txn.Set(ownerHistoryKey, historyBytes); err != nil {
			return err
		}

		// Index for spender's history
		spenderHistoryKey := []byte(fmt.Sprintf("tidx-token-to-%s-%s-%s", spender, timestampStr, history.TxHash))
		if err := txn.Set(spenderHistoryKey, historyBytes); err != nil {
			return err
		}

		return nil
	})
}

// checkTokenHolderBalance verifies that a given address holds a non-zero balance of a token.
// This must be called from within a Badger transaction.
func checkTokenHolderBalance(txn *badger.Txn, tokenAddress, holderAddress string) error {
	balanceKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, tokenAddress, holderAddress))
	balanceItem, err := txn.Get(balanceKey)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("address does not hold any tokens")
		}
		return err // Another DB error
	}

	var balance uint64
	if err := balanceItem.Value(func(val []byte) error {
		if len(val) == 8 {
			balance = binary.BigEndian.Uint64(val)
			return nil
		}
		return fmt.Errorf("invalid balance data for token %s", tokenAddress)
	}); err != nil {
		return err
	}

	if balance == 0 {
		return fmt.Errorf("address holds a zero balance of the token")
	}

	return nil
}

// SetBurnableConfig updates burn configuration (only if not locked)
func (c *Client) SetBurnableConfig(tokenAddress, requester string, config types.BurnConfig, txHash string) error {
	println("config : %s", config.BurnRatePerTx)
	println("config : %s", config.Enabled)
	println("config : %s", config.ManualBurn)
	return c.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(types.TokenPrefix + tokenAddress))
		if err != nil {
			return fmt.Errorf("token not found")
		}

		var token types.Token
		if err := item.Value(func(val []byte) error {
			return jsoniter.Unmarshal(val, &token)
		}); err != nil {
			return err
		}

		if token.Creator != requester {
			return fmt.Errorf("only creator can modify config")
		}

		// Validator side token ownership check: creator must hold a balance
		if err := checkTokenHolderBalance(txn, tokenAddress, requester); err != nil {
			return fmt.Errorf("token creator ownership check failed: %w", err)
		}

		if token.ConfigLocked {
			return fmt.Errorf("config is locked")
		}

		// NEW: Validate burn config
		if config.BurnRatePerTx < 0 || config.BurnRatePerTx > 100 {
			return fmt.Errorf("burn rate must be between 0 and 100")
		}

		token.BurnConfig = config
		tokenBytes, _ := jsoniter.Marshal(&token)
		if err := txn.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes); err != nil {
			return err
		}

		// NEW: Record config change in history
		history := types.TokenHistory{
			TokenAddress: tokenAddress,
			Operation:    types.OpConfigBurn, // New operation type for config changes
			From:         requester,
			To:           tokenAddress, // Config change affects the token itself
			Amount:       0,
			Timestamp:    time.Now(),
			TxHash:       txHash,
		}
		historyBytes, _ := jsoniter.Marshal(history)

		timestampStr := history.Timestamp.Format(time.RFC3339Nano)
		historyKey := []byte(fmt.Sprintf("tidx-token-from-%s-%s-%s", requester, timestampStr, history.TxHash))
		if err := txn.Set(historyKey, historyBytes); err != nil {
			return err
		}

		return nil
	})
}

// SetMintableConfig updates mint configuration (only if not locked)
func (c *Client) SetMintableConfig(tokenAddress, requester string, config types.MintConfig, txHash string) error {
	println("config : %s", config.MintRatePerTx)
	println("config : %s", config.Enabled)
	println("config : %s", config.ManualMint)
	return c.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(types.TokenPrefix + tokenAddress))
		if err != nil {
			return fmt.Errorf("token not found")
		}

		var token types.Token
		if err := item.Value(func(val []byte) error {
			return jsoniter.Unmarshal(val, &token)
		}); err != nil {
			return err
		}

		if token.Creator != requester {
			return fmt.Errorf("only creator can modify config")
		}

		// Validator side token ownership check: creator must hold a balance
		if err := checkTokenHolderBalance(txn, tokenAddress, requester); err != nil {
			return fmt.Errorf("token creator ownership check failed: %w", err)
		}

		if token.ConfigLocked {
			return fmt.Errorf("config is locked")
		}

		// NEW: Validate mint config
		if config.MintRatePerTx < 0 || config.MintRatePerTx > 100 {
			return fmt.Errorf("mint rate must be between 0 and 100")
		}

		token.MintConfig = config
		tokenBytes, _ := jsoniter.Marshal(&token)
		if err := txn.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes); err != nil {
			return err
		}

		// NEW: Record config change in history
		history := types.TokenHistory{
			TokenAddress: tokenAddress,
			Operation:    types.OpConfigMint, // New operation type for config changes
			From:         requester,
			To:           tokenAddress,
			Amount:       0,
			Timestamp:    time.Now(),
			TxHash:       txHash,
		}
		historyBytes, _ := jsoniter.Marshal(history)

		timestampStr := history.Timestamp.Format(time.RFC3339Nano)
		historyKey := []byte(fmt.Sprintf("tidx-token-from-%s-%s-%s", requester, timestampStr, history.TxHash))
		if err := txn.Set(historyKey, historyBytes); err != nil {
			return err
		}

		return nil
	})
}

// LockConfig permanently locks burn/mint configuration
func (c *Client) LockTokenConfig(tokenAddress, requester string, txHash string) error {
	return c.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(types.TokenPrefix + tokenAddress))
		if err != nil {
			return fmt.Errorf("token not found")
		}

		var token types.Token
		if err := item.Value(func(val []byte) error {
			return jsoniter.Unmarshal(val, &token)
		}); err != nil {
			return err
		}

		if token.Creator != requester {
			return fmt.Errorf("only creator can lock config")
		}

		// Validator side token ownership check: creator must hold a balance
		if err := checkTokenHolderBalance(txn, tokenAddress, requester); err != nil {
			return fmt.Errorf("token creator ownership check failed: %w", err)
		}

		// NEW: Check if already locked
		if token.ConfigLocked {
			return fmt.Errorf("config is already locked")
		}

		token.ConfigLocked = true
		tokenBytes, _ := jsoniter.Marshal(&token)
		if err := txn.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes); err != nil {
			return err
		}

		// NEW: Record config lock in history
		history := types.TokenHistory{
			TokenAddress: tokenAddress,
			Operation:    types.OpConfigLock, // New operation type for locking
			From:         requester,
			To:           tokenAddress,
			Amount:       0,
			Timestamp:    time.Now(),
			TxHash:       txHash,
		}
		historyBytes, _ := jsoniter.Marshal(history)

		timestampStr := history.Timestamp.Format(time.RFC3339Nano)
		historyKey := []byte(fmt.Sprintf("tidx-token-from-%s-%s-%s", requester, timestampStr, history.TxHash))
		if err := txn.Set(historyKey, historyBytes); err != nil {
			return err
		}

		return nil
	})
}

// ManualBurn allows manual burning of tokens
func (c *Client) ManualBurnToken(tokenAddress, from string, amount uint64, txHash string) error {
	return c.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(types.TokenPrefix + tokenAddress))
		if err != nil {
			return fmt.Errorf("token not found")
		}

		var token types.Token
		if err := item.Value(func(val []byte) error {
			return jsoniter.Unmarshal(val, &token)
		}); err != nil {
			return err
		}

		if !token.BurnConfig.Enabled || !token.BurnConfig.ManualBurn {
			return fmt.Errorf("manual burn not enabled")
		}

		balanceKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, tokenAddress, from))
		balanceItem, err := txn.Get(balanceKey)
		if err != nil {
			return fmt.Errorf("insufficient balance")
		}

		var balance uint64
		balanceItem.Value(func(val []byte) error {
			balance = binary.BigEndian.Uint64(val)
			return nil
		})

		if balance < amount {
			return fmt.Errorf("insufficient balance")
		}

		newBalance := balance - amount
		balanceBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(balanceBytes, newBalance)
		txn.Set(balanceKey, balanceBytes)

		token.TotalSupply -= amount
		tokenBytes, _ := jsoniter.Marshal(&token)
		txn.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes)

		// FIXED: Record history with PROPER INDEXING
		history := types.TokenHistory{
			TokenAddress: tokenAddress,
			Operation:    types.OpBurn,
			From:         from,
			To:           "0x0",
			Amount:       amount,
			Timestamp:    time.Now(),
			TxHash:       txHash,
		}
		historyBytes, _ := jsoniter.Marshal(history)

		// NEW: Write with proper indexes
		timestampStr := history.Timestamp.Format(time.RFC3339Nano)

		// Index for burner's history
		fromHistoryKey := []byte(fmt.Sprintf("tidx-token-from-%s-%s-%s", from, timestampStr, txHash))
		if err := txn.Set(fromHistoryKey, historyBytes); err != nil {
			return err
		}

		return nil
	})
}

// ManualMintToken allows manual minting of tokens
func (c *Client) ManualMintToken(tokenAddress, requester, to string, amount uint64, txHash string) error {
	return c.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(types.TokenPrefix + tokenAddress))
		if err != nil {
			return fmt.Errorf("token not found")
		}

		var token types.Token
		if err := item.Value(func(val []byte) error {
			return jsoniter.Unmarshal(val, &token)
		}); err != nil {
			return err
		}

		if token.Creator != requester {
			return fmt.Errorf("only creator can mint tokens")
		}

		if err := checkTokenHolderBalance(txn, tokenAddress, requester); err != nil {
			return fmt.Errorf("token creator ownership check failed: %w", err)
		}

		if !token.MintConfig.Enabled || !token.MintConfig.ManualMint {
			return fmt.Errorf("manual mint not enabled")
		}

		balanceKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenBalancePrefix, tokenAddress, to))
		var balance uint64
		balanceItem, err := txn.Get(balanceKey)
		if err == nil {
			balanceItem.Value(func(val []byte) error {
				balance = binary.BigEndian.Uint64(val)
				return nil
			})
		}

		newBalance := balance + amount
		balanceBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(balanceBytes, newBalance)
		txn.Set(balanceKey, balanceBytes)

		token.TotalSupply += amount
		tokenBytes, _ := jsoniter.Marshal(&token)
		txn.Set([]byte(types.TokenPrefix+tokenAddress), tokenBytes)

		ownerKey := []byte(fmt.Sprintf("%s%s-%s", types.TokenOwnerPrefix, to, tokenAddress))
		if _, err := txn.Get(ownerKey); err == badger.ErrKeyNotFound {
			txn.Set(ownerKey, []byte{1})
		}

		// FIXED: Record history with PROPER INDEXING
		history := types.TokenHistory{
			TokenAddress: tokenAddress,
			Operation:    types.OpMint,
			From:         "0x0",
			To:           to,
			Amount:       amount,
			Timestamp:    time.Now(),
			TxHash:       txHash,
		}
		historyBytes, _ := jsoniter.Marshal(history)

		// NEW: Write with proper indexes
		timestampStr := history.Timestamp.Format(time.RFC3339Nano)

		// Index for recipient's history
		toHistoryKey := []byte(fmt.Sprintf("tidx-token-to-%s-%s-%s", to, timestampStr, txHash))
		if err := txn.Set(toHistoryKey, historyBytes); err != nil {
			return err
		}

		return nil
	})
}

// generateTokenAddress creates a deterministic token address
func (c *Client) generateTokenAddress(creator, name, ticker string, timestamp time.Time) string {
	data := fmt.Sprintf("%s:%s:%s:%d", creator, name, ticker, timestamp.UnixNano())
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("0xTKN%x", hash[:18])
}

// Helper function to split keys
func splitKey(key, prefix string) []string {
	if len(key) <= len(prefix) {
		return []string{}
	}
	remainder := key[len(prefix):]
	return []string{remainder}
}

func (c *Client) GetTokenHistory(address string, tokenAddress string, page, limit int) ([]types.TokenHistory, error) {
	if limit <= 0 {
		limit = 10
	}
	if page <= 0 {
		page = 1
	}

	var allHistory []types.TokenHistory

	err := c.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		// Scan transactions where address is sender
		fromPrefix := []byte(fmt.Sprintf("tidx-token-from-%s-", address))
		for it.Seek(fromPrefix); it.ValidForPrefix(fromPrefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var history types.TokenHistory
				if err := jsoniter.Unmarshal(val, &history); err == nil {
					// Filter by token address if specified
					if tokenAddress == "" || history.TokenAddress == tokenAddress {
						allHistory = append(allHistory, history)
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		// Scan transactions where address is receiver
		toPrefix := []byte(fmt.Sprintf("tidx-token-to-%s-", address))
		for it.Seek(toPrefix); it.ValidForPrefix(toPrefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var history types.TokenHistory
				if err := jsoniter.Unmarshal(val, &history); err == nil {
					// Filter by token address if specified
					if tokenAddress == "" || history.TokenAddress == tokenAddress {
						// Check for duplicates (tx might appear in both from and to)
						isDuplicate := false
						for _, existing := range allHistory {
							if existing.TxHash == history.TxHash {
								isDuplicate = true
								break
							}
						}
						if !isDuplicate {
							allHistory = append(allHistory, history)
						}
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get token history: %w", err)
	}

	// Sort by timestamp (most recent first)
	sort.Slice(allHistory, func(i, j int) bool {
		return allHistory[i].Timestamp.After(allHistory[j].Timestamp)
	})

	// Apply pagination
	start := (page - 1) * limit
	if start >= len(allHistory) {
		return []types.TokenHistory{}, nil
	}
	end := start + limit
	if end > len(allHistory) {
		end = len(allHistory)
	}

	return allHistory[start:end], nil
}
