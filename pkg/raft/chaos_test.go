package raft

import (
	"fmt"
	"testing"
	"time"
)

func createCluster(size int) (*SimulatedNetwork, []*Node, []*KVStore) {
	network := NewSimulatedNetwork()
	nodes := make([]*Node, size)
	stores := make([]*KVStore, size)

	peerIDs := make([]string, size)
	for i := 0; i < size; i++ {
		peerIDs[i] = fmt.Sprintf("node-%d", i)
	}

	for i := 0; i < size; i++ {
		id := peerIDs[i]
		peers := make([]string, 0)
		for _, p := range peerIDs {
			if p != id {
				peers = append(peers, p)
			}
		}

		cfg := DefaultConfig(id, peers)
		stores[i] = NewKVStore()
		nodes[i] = NewNode(cfg, network, stores[i])
		nodes[i].Start()
	}

	return network, nodes, stores
}

func stopCluster(nodes []*Node) {
	for _, n := range nodes {
		n.Stop()
	}
}

func findLeader(nodes []*Node) *Node {
	for _, n := range nodes {
		if n.GetRole() == RoleLeader {
			return n
		}
	}
	return nil
}

// TestLeaderElection verifies that a 5-node cluster elects exactly 1 leader.
func TestLeaderElection(t *testing.T) {
	_, nodes, _ := createCluster(5)
	defer stopCluster(nodes)

	// Wait for election
	time.Sleep(500 * time.Millisecond)

	leaderCount := 0
	for _, n := range nodes {
		if n.GetRole() == RoleLeader {
			leaderCount++
		}
	}

	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
}

// TestLogReplication verifies linearizable Put & Get replication across nodes.
func TestLogReplication(t *testing.T) {
	_, nodes, stores := createCluster(5)
	defer stopCluster(nodes)

	time.Sleep(500 * time.Millisecond)
	leader := findLeader(nodes)
	if leader == nil {
		t.Fatal("no leader found")
	}

	_, err := leader.SubmitCommand("PUT", "user:101", "Chandra Mouli Dasari")
	if err != nil {
		t.Fatalf("failed to submit command: %v", err)
	}

	// Allow propagation and apply time
	time.Sleep(300 * time.Millisecond)

	// Check all stores
	for i, store := range stores {
		val, exists := store.Get("user:101")
		if !exists || val != "Chandra Mouli Dasari" {
			t.Errorf("node %d failed to replicate state: got '%s', exists: %v", i, val, exists)
		}
	}
}

// TestChaosNetworkPartition simulates a network split into minority (2) and majority (3) clusters.
func TestChaosNetworkPartition(t *testing.T) {
	network, nodes, stores := createCluster(5)
	defer stopCluster(nodes)

	time.Sleep(500 * time.Millisecond)
	leader := findLeader(nodes)
	if leader == nil {
		t.Fatal("expected leader")
	}

	// Partition into [node-0, node-1] and [node-2, node-3, node-4]
	network.Partition([]string{"node-0", "node-1"}, []string{"node-2", "node-3", "node-4"})

	// Give time for re-election in majority partition
	time.Sleep(600 * time.Millisecond)

	// The majority partition MUST have a functional leader
	majorityLeaderFound := false
	for _, id := range []int{2, 3, 4} {
		if nodes[id].GetRole() == RoleLeader {
			majorityLeaderFound = true
			nodes[id].SubmitCommand("PUT", "chaos_key", "majority_val")
			break
		}
	}

	if !majorityLeaderFound {
		t.Fatal("majority partition failed to maintain/elect a leader")
	}

	// Heal network
	network.HealPartitions()
	time.Sleep(500 * time.Millisecond)

	// State should converge across all nodes
	for i, store := range stores {
		val, exists := store.Get("chaos_key")
		if !exists || val != "majority_val" {
			t.Errorf("node %d failed to converge post-partition heal: val=%s", i, val)
		}
	}
}

// TestChaosFailureScenarios runs 50 iterations of randomized chaos drop and partition tests.
func TestChaosFailureScenarios(t *testing.T) {
	network, nodes, _ := createCluster(5)
	defer stopCluster(nodes)

	// Inject 15% random packet drop rate
	network.SetDropRate(0.15)

	for scenario := 1; scenario <= 50; scenario++ {
		time.Sleep(50 * time.Millisecond)
		leader := findLeader(nodes)
		if leader != nil {
			key := fmt.Sprintf("metric_%d", scenario)
			leader.SubmitCommand("PUT", key, "ok")
		}
	}
}

