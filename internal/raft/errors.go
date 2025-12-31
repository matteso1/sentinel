package raft

import "errors"

var (
	// ErrNotLeader is returned when an operation requires the leader.
	ErrNotLeader = errors.New("not the leader")

	// ErrStopped is returned when the node has been stopped.
	ErrStopped = errors.New("node stopped")
)
