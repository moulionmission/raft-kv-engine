package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Transport defines the communication contract between Raft nodes.
type Transport interface {
	SendRequestVote(target string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(target string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// SimulatedNetwork provides in-memory RPC transport with chaos engineering capabilities.
type SimulatedNetwork struct {
	mu         sync.RWMutex
	nodes      map[string]*Node
	dropRate   float64           // 0.0 to 1.0 probability of packet loss
	partitions map[string]string // nodeID -> partition group name
	latency    time.Duration
}

// NewSimulatedNetwork creates an environment for deterministic multi-node chaos testing.
func NewSimulatedNetwork() *SimulatedNetwork {
	return &SimulatedNetwork{
		nodes:      make(map[string]*Node),
		partitions: make(map[string]string),
	}
}

// Register connects a node to the simulated mesh.
func (sn *SimulatedNetwork) Register(node *Node) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.nodes[node.id] = node
}

// SetDropRate sets synthetic packet drop probability.
func (sn *SimulatedNetwork) SetDropRate(rate float64) {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.dropRate = rate
}

// Partition isolates groups of nodes to simulate split-brain scenarios.
func (sn *SimulatedNetwork) Partition(groupA []string, groupB []string) {
	sn.mu.Lock()
	defer sn.mu.Unlock()

	for _, id := range groupA {
		sn.partitions[id] = "groupA"
	}
	for _, id := range groupB {
		sn.partitions[id] = "groupB"
	}
}

// HealPartitions restores full network connectivity across all nodes.
func (sn *SimulatedNetwork) HealPartitions() {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	sn.partitions = make(map[string]string)
}

func (sn *SimulatedNetwork) canCommunicate(from, to string) bool {
	sn.mu.RLock()
	defer sn.mu.RUnlock()

	if sn.dropRate > 0 && rand.Float64() < sn.dropRate {
		return false
	}

	pFrom, hasFrom := sn.partitions[from]
	pTo, hasTo := sn.partitions[to]

	if hasFrom && hasTo && pFrom != pTo {
		return false // Partition boundary
	}
	return true
}

func (sn *SimulatedNetwork) SendRequestVote(from, target string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	if !sn.canCommunicate(from, target) {
		return nil, errors.New("network dropped packet")
	}

	sn.mu.RLock()
	targetNode, exists := sn.nodes[target]
	sn.mu.RUnlock()

	if !exists {
		return nil, errors.New("target node not found")
	}

	var reply RequestVoteReply
	targetNode.HandleRequestVote(args, &reply)
	return &reply, nil
}

func (sn *SimulatedNetwork) SendAppendEntries(from, target string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if !sn.canCommunicate(from, target) {
		return nil, errors.New("network dropped packet")
	}

	sn.mu.RLock()
	targetNode, exists := sn.nodes[target]
	sn.mu.RUnlock()

	if !exists {
		return nil, errors.New("target node not found")
	}

	var reply AppendEntriesReply
	targetNode.HandleAppendEntries(args, &reply)
	return &reply, nil
}
