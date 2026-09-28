package kademlia

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
)

// MaxSupportedValueSize leaves room for base64 encoding and RPC headers inside
// MaxDatagramSize. The default application limit remains 1024 bytes.
const MaxSupportedValueSize = 32 * 1024

var (
	ErrHashMismatch  = errors.New("value does not match its SHA-256 key")
	ErrValueTooLarge = errors.New("value exceeds the configured size limit")
)

// valueStore owns all stored byte slices. There is no expiration or persistence.
type valueStore struct {
	mu           sync.RWMutex
	values       map[KademliaID][]byte
	maxValueSize int
}

func newValueStore(maxValueSize int) *valueStore {
	return &valueStore{values: make(map[KademliaID][]byte), maxValueSize: maxValueSize}
}

func (store *valueStore) put(key KademliaID, data []byte) error {
	if len(data) > store.maxValueSize {
		return fmt.Errorf("%w: maximum is %d bytes", ErrValueTooLarge, store.maxValueSize)
	}
	value := append([]byte(nil), data...)
	if KademliaID(sha256.Sum256(value)) != key {
		return ErrHashMismatch
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.values[key] = value
	return nil
}

// get distinguishes a stored empty value from a missing key and returns a copy.
// Distributed retrieval uses this helper before querying peers.
func (store *valueStore) get(key KademliaID) ([]byte, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	value, found := store.values[key]
	return append([]byte(nil), value...), found
}

// snapshot copies values under the lock, then releases it before network work.
// Values arriving during a pass are included in the next pass.
func (store *valueStore) snapshot() map[KademliaID][]byte {
	store.mu.RLock()
	defer store.mu.RUnlock()
	result := make(map[KademliaID][]byte, len(store.values))
	for key, data := range store.values {
		result[key] = append([]byte(nil), data...)
	}
	return result
}
