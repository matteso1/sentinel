package raft

import (
	"sync/atomic"
)

// VoteRequest is sent by candidates to request votes.
type VoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
	ResponseChan chan VoteResponse
}

// VoteResponse is the reply to a vote request.
type VoteResponse struct {
	Term        uint64
	VoteGranted bool
	VoterID     string
}

// startElection begins a new election.
func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id
	n.setState(Candidate)
	currentTerm := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	n.mu.Unlock()

	// Vote for self
	votesReceived := int32(1)
	votesNeeded := len(n.peers)/2 + 1

	// If no peers, we automatically become leader
	if len(n.peers) == 0 {
		n.becomeLeader()
		return
	}

	// Request votes from all peers
	responseChan := make(chan VoteResponse, len(n.peers))

	for _, peer := range n.peers {
		go func(peerID string) {
			// In production, this would be an RPC call
			// For now, simulate with channels
			_ = VoteRequest{
				Term:         currentTerm,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
				ResponseChan: responseChan,
			}
			// Simulate sending to peer
			n.voteRespChan <- VoteResponse{
				Term:        currentTerm,
				VoteGranted: true, // Simulated
				VoterID:     peerID,
			}
		}(peer)
	}

	// Collect votes
	for range n.peers {
		select {
		case resp := <-n.voteRespChan:
			if resp.Term > currentTerm {
				// Discovered higher term, step down
				n.mu.Lock()
				n.currentTerm = resp.Term
				n.votedFor = ""
				n.setState(Follower)
				n.mu.Unlock()
				return
			}
			if resp.VoteGranted && resp.Term == currentTerm {
				if int(atomic.AddInt32(&votesReceived, 1)) >= votesNeeded {
					n.becomeLeader()
					return
				}
			}
		case <-n.stopChan:
			return
		}
	}
}

// becomeLeader transitions to leader state.
func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.setState(Leader)

	// Initialize leader state
	lastLogIndex, _ := n.lastLogInfo()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastLogIndex + 1
		n.matchIndex[peer] = 0
	}
}

// handleVoteRequest processes an incoming vote request.
func (n *Node) handleVoteRequest(req VoteRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	response := VoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
		VoterID:     n.id,
	}

	// If request term is old, reject
	if req.Term < n.currentTerm {
		if req.ResponseChan != nil {
			req.ResponseChan <- response
		}
		return
	}

	// If request term is newer, update our term
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
		n.setState(Follower)
	}

	// Check if we can vote for this candidate
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	logUpToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if (n.votedFor == "" || n.votedFor == req.CandidateID) && logUpToDate {
		n.votedFor = req.CandidateID
		response.VoteGranted = true
		n.lastHeartbeat = n.lastHeartbeat // Reset election timeout
	}

	response.Term = n.currentTerm
	if req.ResponseChan != nil {
		req.ResponseChan <- response
	}
}

// lastLogInfo returns the index and term of the last log entry.
func (n *Node) lastLogInfo() (uint64, uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// RequestVote is called by other nodes to request a vote.
func (n *Node) RequestVote(req VoteRequest) VoteResponse {
	responseChan := make(chan VoteResponse, 1)
	req.ResponseChan = responseChan
	n.voteChan <- req
	return <-responseChan
}
