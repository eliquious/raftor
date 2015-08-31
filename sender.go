package raftor

import "github.com/coreos/etcd/raft/raftpb"

type Sender interface {
	Send([]raftpb.Message)
}
