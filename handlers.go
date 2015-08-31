package raftor

import "github.com/coreos/etcd/raft/raftpb"

// RemoteMessageHandler handles messages from a remote node.
type RemoteMessageHandler interface {

	// HandleRemoteMessage is called when a message is recieved from a remote node.
	HandleRemoteMessage(msg raftpb.Message)
}
