package raftor

import "github.com/coreos/etcd/raft"

// ClusterConfig helps to configure a RaftNode
type ClusterConfig struct {
	Name                   string
	LocalNodeName          string
	SnapshotCount          uint64
	NumberOfCatchUpEntries uint64
	SnapshotStorage        SnapshotStorage
	Store                  Store
	Applier                Applier
	RaftNodeFactory        RaftNodeFactory
	TransporterFactory     TransporterFactory
	Raft                   raft.Config
}
