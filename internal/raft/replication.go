package raft

// AppendEntriesRequest is sent by leaders to replicate log entries and as heartbeats.
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
	ResponseChan chan AppendEntriesResponse
}

// AppendEntriesResponse is the reply to an AppendEntries RPC.
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	NodeID  string
	// For fast backup
	ConflictIndex uint64
	ConflictTerm  uint64
}

// sendHeartbeats sends empty AppendEntries to all peers.
func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	currentTerm := n.currentTerm
	commitIndex := n.commitIndex
	n.mu.RUnlock()

	for _, peer := range n.peers {
		go func(peerID string) {
			n.mu.RLock()
			prevLogIndex := n.nextIndex[peerID] - 1
			prevLogTerm := uint64(0)
			if prevLogIndex > 0 && int(prevLogIndex) <= len(n.log) {
				prevLogTerm = n.log[prevLogIndex-1].Term
			}

			// Get entries to send
			entries := make([]LogEntry, 0)
			if int(n.nextIndex[peerID]) <= len(n.log) {
				entries = n.log[n.nextIndex[peerID]-1:]
			}
			n.mu.RUnlock()

			req := AppendEntriesRequest{
				Term:         currentTerm,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			}

			// In production, this would be an RPC
			n.appendRespChan <- AppendEntriesResponse{
				Term:    currentTerm,
				Success: true,
				NodeID:  peerID,
			}
			_ = req
		}(peer)
	}

	// Process responses
	for range n.peers {
		select {
		case resp := <-n.appendRespChan:
			n.handleAppendEntriesResponse(resp)
		case <-n.stopChan:
			return
		default:
			continue
		}
	}
}

// handleAppendEntries processes an incoming AppendEntries request.
func (n *Node) handleAppendEntries(req AppendEntriesRequest) {
	n.mu.Lock()
	defer n.mu.Unlock()

	response := AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
		NodeID:  n.id,
	}

	// If leader's term is less than ours, reject
	if req.Term < n.currentTerm {
		if req.ResponseChan != nil {
			req.ResponseChan <- response
		}
		return
	}

	// If we see a higher term, step down
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.votedFor = ""
	}

	n.setState(Follower)
	n.lastHeartbeat = n.lastHeartbeat // Reset election timeout

	// Check if log contains an entry at prevLogIndex with prevLogTerm
	if req.PrevLogIndex > 0 {
		if int(req.PrevLogIndex) > len(n.log) {
			// Missing entries
			response.ConflictIndex = uint64(len(n.log)) + 1
			if req.ResponseChan != nil {
				req.ResponseChan <- response
			}
			return
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			// Conflicting entry
			response.ConflictTerm = n.log[req.PrevLogIndex-1].Term
			// Find first entry with conflicting term
			for i := int(req.PrevLogIndex) - 1; i >= 0; i-- {
				if n.log[i].Term != response.ConflictTerm {
					response.ConflictIndex = uint64(i + 2)
					break
				}
				response.ConflictIndex = uint64(i + 1)
			}
			if req.ResponseChan != nil {
				req.ResponseChan <- response
			}
			return
		}
	}

	// Append new entries (removing any conflicting ones)
	for i, entry := range req.Entries {
		index := req.PrevLogIndex + uint64(i) + 1
		if int(index) <= len(n.log) {
			if n.log[index-1].Term != entry.Term {
				// Truncate log from here
				n.log = n.log[:index-1]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	// Update commit index
	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := req.PrevLogIndex + uint64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
		n.applyCommitted()
	}

	response.Success = true
	response.Term = n.currentTerm
	if req.ResponseChan != nil {
		req.ResponseChan <- response
	}
}

// handleAppendEntriesResponse processes a response from a follower.
func (n *Node) handleAppendEntriesResponse(resp AppendEntriesResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.currentTerm = resp.Term
		n.votedFor = ""
		n.setState(Follower)
		return
	}

	if n.State() != Leader || resp.Term != n.currentTerm {
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex
		if resp.ConflictIndex > 0 {
			n.nextIndex[resp.NodeID] = resp.ConflictIndex
		}
		n.matchIndex[resp.NodeID] = n.nextIndex[resp.NodeID] - 1
		n.nextIndex[resp.NodeID]++

		// Try to advance commit index
		n.advanceCommitIndex()
	} else {
		// Back off nextIndex
		if resp.ConflictIndex > 0 {
			n.nextIndex[resp.NodeID] = resp.ConflictIndex
		} else {
			n.nextIndex[resp.NodeID]--
		}
	}
}

// advanceCommitIndex checks if we can advance the commit index.
func (n *Node) advanceCommitIndex() {
	// Find the highest index replicated on majority
	for i := len(n.log); i > int(n.commitIndex); i-- {
		if n.log[i-1].Term != n.currentTerm {
			continue
		}

		count := 1 // Count self
		for _, matchIdx := range n.matchIndex {
			if matchIdx >= uint64(i) {
				count++
			}
		}

		if count > len(n.peers)/2 {
			n.commitIndex = uint64(i)
			n.applyCommitted()
			break
		}
	}
}

// applyCommitted applies all committed but not yet applied entries.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied-1]
		if n.onCommit != nil {
			n.onCommit(entry)
		}
	}
}

// AppendEntries is called by the leader to replicate log entries.
func (n *Node) AppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	responseChan := make(chan AppendEntriesResponse, 1)
	req.ResponseChan = responseChan
	n.appendChan <- req
	return <-responseChan
}

// Propose proposes a new command to the replicated log (leader only).
func (n *Node) Propose(command []byte) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State() != Leader {
		return 0, ErrNotLeader
	}

	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log)) + 1,
		Command: command,
	}
	n.log = append(n.log, entry)

	return entry.Index, nil
}
