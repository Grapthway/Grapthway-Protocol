package state

import (
	"fmt"
	"grapthway/pkg/ledger/types"
	"sync"
	"time"
)

// Token cache for performance optimization
type TokenCache struct {
	sync.RWMutex
	tokens     map[string]*types.Token
	ttl        time.Duration
	timestamps map[string]time.Time
}

func NewTokenCache(ttl time.Duration) *TokenCache {
	tc := &TokenCache{
		tokens:     make(map[string]*types.Token),
		timestamps: make(map[string]time.Time),
		ttl:        ttl,
	}
	go tc.cleanup()
	return tc
}

func (tc *TokenCache) Get(address string) (*types.Token, bool) {
	tc.RLock()
	defer tc.RUnlock()

	token, exists := tc.tokens[address]
	if !exists {
		return nil, false
	}

	// Check if expired
	if time.Since(tc.timestamps[address]) > tc.ttl {
		return nil, false
	}

	return token, true
}

func (tc *TokenCache) Set(address string, token *types.Token) {
	tc.Lock()
	defer tc.Unlock()
	tc.tokens[address] = token
	tc.timestamps[address] = time.Now()
}

// ✅ NEW: Delete token from cache (for invalidation)
func (tc *TokenCache) Delete(address string) {
	tc.Lock()
	defer tc.Unlock()
	delete(tc.tokens, address)
	delete(tc.timestamps, address)
}

// ✅ NEW: Update token in cache (for block processing modifications)
func (tc *TokenCache) Update(address string, updateFn func(*types.Token) error) error {
	tc.Lock()
	defer tc.Unlock()

	token, exists := tc.tokens[address]
	if !exists {
		return fmt.Errorf("token not in cache")
	}

	// Create a copy to avoid mutation issues
	tokenCopy := *token
	if err := updateFn(&tokenCopy); err != nil {
		return err
	}

	tc.tokens[address] = &tokenCopy
	tc.timestamps[address] = time.Now()
	return nil
}

// ✅ NEW: Batch load tokens for block processing
func (tc *TokenCache) BatchLoad(addresses []string, loadFn func(string) (*types.Token, error)) error {
	tc.Lock()
	defer tc.Unlock()

	for _, addr := range addresses {
		// Skip if already cached and not expired
		if _, exists := tc.tokens[addr]; exists {
			if time.Since(tc.timestamps[addr]) <= tc.ttl {
				continue
			}
		}

		// Load from database
		token, err := loadFn(addr)
		if err != nil {
			// Token might not exist yet (will be created in this block)
			continue
		}

		tc.tokens[addr] = token
		tc.timestamps[addr] = time.Now()
	}

	return nil
}

// ✅ NEW: Clear all cache (for hard resets)
func (tc *TokenCache) Clear() {
	tc.Lock()
	defer tc.Unlock()
	tc.tokens = make(map[string]*types.Token)
	tc.timestamps = make(map[string]time.Time)
}

func (tc *TokenCache) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		tc.Lock()
		now := time.Now()
		for addr, ts := range tc.timestamps {
			if now.Sub(ts) > tc.ttl {
				delete(tc.tokens, addr)
				delete(tc.timestamps, addr)
			}
		}
		tc.Unlock()
	}
}
