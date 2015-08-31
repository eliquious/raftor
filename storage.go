package raftor

import "github.com/coreos/etcd/raft/raftpb"

type SnapshotStorage interface {

	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st raftpb.HardState, ents []raftpb.Entry) error

	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap raftpb.Snapshot) error

	// Close closes the Storage and performs finalization.
	Close() error
}
