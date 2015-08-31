package raftor

import "github.com/coreos/etcd/raft/raftpb"

type Sender interface {

	// Send sends out the given messages to the remote peers.
	// Each message has a To field, which is an id that maps
	// to an existing peer in the transport.
	// If the id cannot be found in the transport, the message
	// will be ignored.
	Send([]raftpb.Message)
}
