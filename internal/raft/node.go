package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

// State represents the current role of a Raft node.
type State int32

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Node implements the Raft consensus algorithm.
// It handles leader election and log replication.
type Node struct {
	// Persistent state (would be persisted to disk in production)
	id          string
	currentTerm uint64
	votedFor    string
	log         []LogEntry

	// Volatile state
	state       State
	commitIndex uint64
	lastApplied uint64

	// Leader volatile state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// Cluster configuration
	peers []string

	// Timing
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration
	lastHeartbeat    time.Time

	// Communication channels
	voteChan       chan VoteRequest
	voteRespChan   chan VoteResponse
	appendChan     chan AppendEntriesRequest
	appendRespChan chan AppendEntriesResponse

	// Control
	stopChan chan struct{}
	mu       sync.RWMutex
	running  atomic.Bool

	// Callbacks
	onStateChange func(State)
	onCommit      func(LogEntry)
}

// LogEntry represents an entry in the replicated log.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// NodeConfig configures a Raft node.
type NodeConfig struct {
	ID               string
	Peers            []string
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	OnStateChange    func(State)
	OnCommit         func(LogEntry)
}

// DefaultNodeConfig returns sensible defaults.
func DefaultNodeConfig(id string) NodeConfig {
	return NodeConfig{
		ID:               id,
		Peers:            []string{},
		ElectionTimeout:  300 * time.Millisecond,
		HeartbeatTimeout: 100 * time.Millisecond,
	}
}

// NewNode creates a new Raft node.
func NewNode(config NodeConfig) *Node {
	return &Node{
		id:               config.ID,
		currentTerm:      0,
		votedFor:         "",
		log:              make([]LogEntry, 0),
		state:            Follower,
		commitIndex:      0,
		lastApplied:      0,
		nextIndex:        make(map[string]uint64),
		matchIndex:       make(map[string]uint64),
		peers:            config.Peers,
		electionTimeout:  config.ElectionTimeout,
		heartbeatTimeout: config.HeartbeatTimeout,
		lastHeartbeat:    time.Now(),
		voteChan:         make(chan VoteRequest, 10),
		voteRespChan:     make(chan VoteResponse, 10),
		appendChan:       make(chan AppendEntriesRequest, 10),
		appendRespChan:   make(chan AppendEntriesResponse, 10),
		stopChan:         make(chan struct{}),
		onStateChange:    config.OnStateChange,
		onCommit:         config.OnCommit,
	}
}

// Start begins the Raft state machine.
func (n *Node) Start() {
	if n.running.Swap(true) {
		return // Already running
	}
	go n.run()
}

// Stop halts the Raft state machine.
func (n *Node) Stop() {
	if !n.running.Swap(false) {
		return
	}
	close(n.stopChan)
}

// State returns the current node state.
func (n *Node) State() State {
	return State(atomic.LoadInt32((*int32)(&n.state)))
}

// IsLeader returns true if this node is the leader.
func (n *Node) IsLeader() bool {
	return n.State() == Leader
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

// ID returns the node ID.
func (n *Node) ID() string {
	return n.id
}

// run is the main Raft loop.
func (n *Node) run() {
	electionTimer := time.NewTimer(n.randomElectionTimeout())
	heartbeatTimer := time.NewTimer(n.heartbeatTimeout)

	for {
		select {
		case <-n.stopChan:
			electionTimer.Stop()
			heartbeatTimer.Stop()
			return

		case <-electionTimer.C:
			if n.State() != Leader {
				n.startElection()
			}
			electionTimer.Reset(n.randomElectionTimeout())

		case <-heartbeatTimer.C:
			if n.State() == Leader {
				n.sendHeartbeats()
			}
			heartbeatTimer.Reset(n.heartbeatTimeout)

		case req := <-n.voteChan:
			n.handleVoteRequest(req)

		case req := <-n.appendChan:
			n.handleAppendEntries(req)
			electionTimer.Reset(n.randomElectionTimeout())
		}
	}
}

// randomElectionTimeout returns a randomized election timeout.
func (n *Node) randomElectionTimeout() time.Duration {
	// Add randomness to prevent split votes
	base := n.electionTimeout
	return base + time.Duration(time.Now().UnixNano()%int64(base/2))
}

// setState atomically changes the node state.
func (n *Node) setState(s State) {
	old := State(atomic.SwapInt32((*int32)(&n.state), int32(s)))
	if old != s && n.onStateChange != nil {
		n.onStateChange(s)
	}
}
