package dht

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
)

// CID represents a Content Identifier, a unique hash for a piece of data.
type CID string

// DHT provides a simple, in-memory implementation of a Distributed Hash Table.
type DHT struct {
	store map[string][]byte // Using a simple string key for flexibility
	lock  sync.RWMutex
	ctx   context.Context
}

// NewDHT creates a new in-memory DHT.
func NewDHT(ctx context.Context) *DHT {
	return &DHT{
		store: make(map[string][]byte),
		ctx:   ctx,
	}
}

// Put stores data in the DHT with a given key and returns its Content Identifier (CID).
func (d *DHT) Put(key string, data []byte) (CID, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	hash := sha256.Sum256(data)
	cid := CID(hex.EncodeToString(hash[:]))

	select {
	case <-d.ctx.Done():
		return "", d.ctx.Err()
	default:
		d.store[key] = data
		return cid, nil
	}
}

// Get retrieves data from the DHT using its key.
func (d *DHT) Get(key string) ([]byte, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	select {
	case <-d.ctx.Done():
		return nil, d.ctx.Err()
	default:
		data, exists := d.store[key]
		if !exists {
			return nil, fmt.Errorf("content with key %s not found", key)
		}
		return data, nil
	}
}

// GetAllWithPrefix retrieves all key-value pairs where the key has a given prefix.
// This is used by the monitoring endpoint to find all workflow checkpoints.
func (d *DHT) GetAllWithPrefix(prefix string) map[string][]byte {
	d.lock.RLock()
	defer d.lock.RUnlock()

	results := make(map[string][]byte)
	for key, value := range d.store {
		if strings.HasPrefix(key, prefix) {
			results[key] = value
		}
	}
	return results
}
