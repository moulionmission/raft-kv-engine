package raft

import "time"

// NodeRole represents the Raft state of a cluster participant.
type NodeRole string

const (
	RoleFollower  NodeRole = "Follower"
	RoleCandidate NodeRole = "Candidate"
	RoleLeader    NodeRole = "Leader"
)

// LogEntry represents an individual command replicated across the cluster.
type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Op    string `json:"op"` // "PUT" or "DELETE"
}

// RequestVoteArgs represents RPC arguments for leader election.
type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteReply represents RPC response for leader election.
type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesArgs represents RPC arguments for log replication and heartbeats.
type AppendEntriesArgs struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"`
}

// AppendEntriesReply represents RPC reply for log replication.
type AppendEntriesReply struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	// Fast log reconciliation optimization:
	ConflictIndex uint64 `json:"conflict_index"`
	ConflictTerm  uint64 `json:"conflict_term"`
}

// Config defines the cluster parameters and timing windows.
type Config struct {
	NodeID             string
	Peers              []string
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

// DefaultConfig returns standard timing configurations for local and distributed clusters.
func DefaultConfig(nodeID string, peers []string) Config {
	return Config{
		NodeID:             nodeID,
		Peers:              peers,
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
	}
}
