package raft

import (
	"testing"
	"time"
)

func TestNode_Creation(t *testing.T) {
	config := DefaultNodeConfig("node-1")
	node := NewNode(config)

	if node.ID() != "node-1" {
		t.Errorf("expected node-1, got %s", node.ID())
	}

	if node.State() != Follower {
		t.Errorf("expected Follower state, got %s", node.State())
	}

	if node.Term() != 0 {
		t.Errorf("expected term 0, got %d", node.Term())
	}
}

func TestNode_BecomesLeaderWithNoPeers(t *testing.T) {
	config := DefaultNodeConfig("node-1")
	config.ElectionTimeout = 50 * time.Millisecond
	config.HeartbeatTimeout = 20 * time.Millisecond

	stateChanges := make([]State, 0)
	config.OnStateChange = func(s State) {
		stateChanges = append(stateChanges, s)
	}

	node := NewNode(config)
	node.Start()
	defer node.Stop()

	// Wait for election
	time.Sleep(200 * time.Millisecond)

	if node.State() != Leader {
		t.Errorf("expected Leader state, got %s", node.State())
	}

	if node.Term() != 1 {
		t.Errorf("expected term 1, got %d", node.Term())
	}
}

func TestNode_ProposeAsLeader(t *testing.T) {
	config := DefaultNodeConfig("node-1")
	config.ElectionTimeout = 50 * time.Millisecond

	committed := make([]LogEntry, 0)
	config.OnCommit = func(entry LogEntry) {
		committed = append(committed, entry)
	}

	node := NewNode(config)
	node.Start()
	defer node.Stop()

	// Wait to become leader
	time.Sleep(200 * time.Millisecond)

	// Propose a command
	index, err := node.Propose([]byte("SET foo bar"))
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 {
		t.Errorf("expected index 1, got %d", index)
	}
}

func TestNode_RejectProposeWhenNotLeader(t *testing.T) {
	config := DefaultNodeConfig("node-1")
	node := NewNode(config)
	// Don't start - stays follower

	_, err := node.Propose([]byte("SET foo bar"))
	if err != ErrNotLeader {
		t.Errorf("expected ErrNotLeader, got %v", err)
	}
}

func TestVoteRequest_GrantsVote(t *testing.T) {
	config := DefaultNodeConfig("node-1")
	node := NewNode(config)
	node.Start()
	defer node.Stop()

	resp := node.RequestVote(VoteRequest{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Error("expected vote to be granted")
	}
}

func TestVoteRequest_RejectsOlderTerm(t *testing.T) {
	config := DefaultNodeConfig("node-1")
	node := NewNode(config)
	node.Start()
	defer node.Stop()

	// First, advance term by receiving a higher term vote request
	node.RequestVote(VoteRequest{
		Term:         5,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	// Now try with older term
	resp := node.RequestVote(VoteRequest{
		Term:         2,
		CandidateID:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Error("expected vote to be rejected for older term")
	}
}
