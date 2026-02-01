package ledger

import (
	"encoding/binary"
	"fmt"
	"grapthway/pkg/ledger/types"
	"log"
	"sort"

	jsoniter "github.com/json-iterator/go"

	"github.com/dgraph-io/badger/v4"
)

// GetToken retrieves a token by address
func (c *Client) GetToken(tokenAddress string) (*types.Token, error) {
	// Check cache first
	if cachedToken, found := c.tokenCache.Get(tokenAddress); found {
		log.Printf("[TOKEN-CACHE] Cache hit for token %s", tokenAddress)
		return cachedToken, nil
	}

	// Cache miss - load from DB
	var token types.Token
	err := c.db.View(func(txn *badger.Txn) error {
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

	// Cache the result
	c.tokenCache.Set(tokenAddress, &token)
	log.Printf("[TOKEN-CACHE] Cached token %s from DB", tokenAddress)

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

// GetTokenAllowance retrieves allowance amount
func (c *Client) GetTokenAllowance(tokenAddress, owner, spender string) (uint64, error) {
	var allowance uint64

	err := c.db.View(func(txn *badger.Txn) error {
		_, err := c.GetTokenFromCacheOrDB(tokenAddress)
		if err != nil {
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

// GetTokenFromCacheOrDB retrieves token from cache first, then DB
func (c *Client) GetTokenFromCacheOrDB(address string) (*types.Token, error) {
	// Try cache first
	if token, found := c.tokenCache.Get(address); found {
		return token, nil
	}

	// Load from database
	token, err := c.GetToken(address)
	if err != nil {
		return nil, err
	}

	// Cache it for future use
	c.tokenCache.Set(address, token)
	return token, nil
}

// UpdateTokenInCache updates token state after block operations
func (c *Client) UpdateTokenInCache(address string, token *types.Token) {
	c.tokenCache.Set(address, token)
}

// InvalidateTokenCache removes token from cache (force reload)
func (c *Client) InvalidateTokenCache(address string) {
	c.tokenCache.Delete(address)
}
