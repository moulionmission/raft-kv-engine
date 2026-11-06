package raft

import (
	"sync"
)

// KVStore is the replicated state machine for the Raft node.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore initializes an empty in-memory key-value store.
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Apply executes a committed Raft LogEntry onto the state machine.
func (kv *KVStore) Apply(entry LogEntry) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch entry.Op {
	case "PUT":
		kv.data[entry.Key] = entry.Value
	case "DELETE":
		delete(kv.data, entry.Key)
	}
}

// Get performs a thread-safe read from the local state machine.
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	val, exists := kv.data[key]
	return val, exists
}

// Snapshot returns an in-memory copy of the entire dataset for checkpointing.
func (kv *KVStore) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	copyMap := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		copyMap[k] = v
	}
	return copyMap
}

// Restore resets the state machine with data from a snapshot.
func (kv *KVStore) Restore(snapshot map[string]string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data = make(map[string]string, len(snapshot))
	for k, v := range snapshot {
		kv.data[k] = v
	}
}
