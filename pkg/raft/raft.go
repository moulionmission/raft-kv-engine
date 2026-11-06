package raft

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Node represents an individual Raft consensus instance.
type Node struct {
	mu        sync.Mutex
	id        string
	peers     []string
	network   *SimulatedNetwork
	config    Config
	store     *KVStore

	// Persistent state
	currentTerm uint64
	votedFor    string
	log         []LogEntry

	// Volatile state on all servers
	commitIndex uint64
	lastApplied uint64
	role        NodeRole

	// Volatile state on leaders
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Control channels
	electionResetEvent time.Time
	stopChan           chan struct{}
	applyChan          chan LogEntry
}

// NewNode constructs and bootstraps a Raft node.
func NewNode(cfg Config, network *SimulatedNetwork, store *KVStore) *Node {
	n := &Node{
		id:          cfg.NodeID,
		peers:       cfg.Peers,
		network:     network,
		config:      cfg,
		store:       store,
		currentTerm: 0,
		votedFor:    "",
		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		role:        RoleFollower,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		stopChan:    make(chan struct{}),
		applyChan:   make(chan LogEntry, 1000),
	}

	// Add dummy index 0 entry for 1-based indexing simplicity
	n.log = append(n.log, LogEntry{Index: 0, Term: 0})
	network.Register(n)

	return n
}

// Start begins the background election timer and applier loop.
func (n *Node) Start() {
	n.mu.Lock()
	n.electionResetEvent = time.Now()
	n.mu.Unlock()

	go n.runElectionTimer()
	go n.runApplyLoop()
}

// Stop cleanly terminates node routines.
func (n *Node) Stop() {
	close(n.stopChan)
}

func (n *Node) lastLogIndexTerm() (uint64, uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) runElectionTimer() {
	timeout := n.randomElectionTimeout()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != RoleLeader && time.Since(n.electionResetEvent) >= timeout {
				n.startElection()
				timeout = n.randomElectionTimeout()
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	d := n.config.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(n.config.ElectionTimeoutMax-n.config.ElectionTimeoutMin)))
	return d
}

func (n *Node) startElection() {
	n.role = RoleCandidate
	n.currentTerm++
	n.votedFor = n.id
	n.electionResetEvent = time.Now()
	term := n.currentTerm
	lastIndex, lastTerm := n.lastLogIndexTerm()

	votesReceived := 1 // Vote for self
	neededQuorum := (len(n.peers)+1)/2 + 1

	for _, peer := range n.peers {
		go func(target string) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}

			reply, err := n.network.SendRequestVote(n.id, target, args)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if n.role == RoleCandidate && reply.Term == n.currentTerm && reply.VoteGranted {
				votesReceived++
				if votesReceived >= neededQuorum {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *Node) becomeLeader() {
	n.role = RoleLeader
	lastIndex, _ := n.lastLogIndexTerm()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1
		n.matchIndex[peer] = 0
	}

	go n.runHeartbeatLoop()
}

func (n *Node) becomeFollower(term uint64) {
	n.role = RoleFollower
	n.currentTerm = term
	n.votedFor = ""
	n.electionResetEvent = time.Now()
}

func (n *Node) runHeartbeatLoop() {
	ticker := time.NewTicker(n.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != RoleLeader {
				n.mu.Unlock()
				return
			}
			n.broadcastAppendEntries()
			n.mu.Unlock()
		}
	}
}

func (n *Node) broadcastAppendEntries() {
	for _, peer := range n.peers {
		go n.sendAppendEntriesToPeer(peer)
	}
}

func (n *Node) sendAppendEntriesToPeer(peer string) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}

	prevIndex := n.nextIndex[peer] - 1
	var prevTerm uint64
	if prevIndex < uint64(len(n.log)) {
		prevTerm = n.log[prevIndex].Term
	}

	entries := make([]LogEntry, 0)
	if uint64(len(n.log)) > n.nextIndex[peer] {
		entries = append(entries, n.log[n.nextIndex[peer]:]...)
	}

	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	currentTerm := n.currentTerm
	n.mu.Unlock()

	reply, err := n.network.SendAppendEntries(n.id, peer, args)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}

	if n.role == RoleLeader && reply.Term == currentTerm {
		if reply.Success {
			n.nextIndex[peer] = args.PrevLogIndex + uint64(len(args.Entries)) + 1
			n.matchIndex[peer] = args.PrevLogIndex + uint64(len(args.Entries))
			n.checkAndAdvanceCommitIndex()
		} else {
			if n.nextIndex[peer] > 1 {
				n.nextIndex[peer]--
			}
		}
	}
}

func (n *Node) checkAndAdvanceCommitIndex() {
	neededQuorum := (len(n.peers)+1)/2 + 1
	lastLogIndex, _ := n.lastLogIndexTerm()

	for N := lastLogIndex; N > n.commitIndex; N-- {
		if n.log[N].Term != n.currentTerm {
			continue
		}

		matchCount := 1 // Leader matches
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= N {
				matchCount++
			}
		}

		if matchCount >= neededQuorum {
			n.commitIndex = N
			break
		}
	}
}

// HandleRequestVote processes incoming vote inquiries.
func (n *Node) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	if args.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == args.CandidateID) {
		lastLogIndex, lastLogTerm := n.lastLogIndexTerm()
		// Raft up-to-date log check
		logOk := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
		if logOk {
			reply.VoteGranted = true
			n.votedFor = args.CandidateID
			n.electionResetEvent = time.Now()
		}
	}
}

// HandleAppendEntries handles heartbeats and log entries from leaders.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Success = false
	reply.Term = n.currentTerm

	if args.Term < n.currentTerm {
		return
	}

	if args.Term > n.currentTerm || n.role == RoleCandidate {
		n.becomeFollower(args.Term)
	}

	n.electionResetEvent = time.Now()

	// Verify log consistency at PrevLogIndex
	if args.PrevLogIndex >= uint64(len(n.log)) || n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// Overwrite differing entries if any
	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		idx := insertIndex + uint64(i)
		if idx < uint64(len(n.log)) {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	// Update commit index
	if args.LeaderCommit > n.commitIndex {
		lastLogIndex, _ := n.lastLogIndexTerm()
		if args.LeaderCommit < lastLogIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastLogIndex
		}
	}

	reply.Success = true
}

func (n *Node) runApplyLoop() {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopChan:
			return
		case <-ticker.C:
			n.mu.Lock()
			for n.commitIndex > n.lastApplied {
				n.lastApplied++
				entry := n.log[n.lastApplied]
				n.store.Apply(entry)
			}
			n.mu.Unlock()
		}
	}
}

// SubmitCommand appends an entry to the leader's log and replicates it.
func (n *Node) SubmitCommand(op, key, val string) (uint64, error) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return 0, errors.New("not the leader")
	}

	entry := LogEntry{
		Index: uint64(len(n.log)),
		Term:  n.currentTerm,
		Key:   key,
		Value: val,
		Op:    op,
	}
	n.log = append(n.log, entry)
	index := entry.Index
	n.broadcastAppendEntries()
	n.mu.Unlock()

	return index, nil
}

// GetRole returns current consensus state.
func (n *Node) GetRole() NodeRole {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// GetTerm returns current term.
func (n *Node) GetTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

