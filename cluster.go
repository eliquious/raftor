package raftor

import (
	"bytes"
	"fmt"
	"time"

	"github.com/blacklabeldata/raftor/pkg/murmur"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"golang.org/x/net/context"
)

// ClusterEventType is an enum describing how the cluster is changing.
type ClusterEventType uint8

const (

	// AddMember is used to describe a cluster change when a node is added.
	AddMember ClusterEventType = iota

	// RemoveMember is used to describe a cluster change when a node is removed.
	RemoveMember

	// UpdateMember is used to describe a cluster change when a node is updated.
	UpdateMember
)

// ClusterChangeEvent is used to store details about a cluster change. It is sent when a new node is detected and after the change has been applied to a raft log.
type ClusterChangeEvent struct {
	Type   ClusterEventType
	Member Member
}

// Cluster maintains an active list of nodes in the cluster. Cluster is also responsible for reporting and responding to changes in cluster membership.
type Cluster interface {
	Updater
	Sender
	Starter

	// ID represents the cluster ID.
	ID() uint64

	// Name returns the Cluster's name.
	Name() string

	// LocalNode returns the RaftNode which represents the local node of the cluster.
	LocalNode() RaftNode

	// Config returns the ClusterConfig which was used to start the cluster.
	Config() ClusterConfig

	// Stop stops the cluster and triggers the context when finished.
	Stop()
}

func NewCluster(cfg *ClusterConfig) (Cluster, error) {
	return &cluster{
		config: cfg,
	}, nil
}

type cluster struct {
	config *ClusterConfig

	raftNode    RaftNode
	transporter Transporter

	context context.Context
	cancel  context.CancelFunc
	errorc  chan error
}

func (c *cluster) ID() uint64 {
	return uint64(murmur.Murmur3([]byte(c.config.Name), murmur.M3Seed))
}

func (c *cluster) Name() string {
	return c.config.Name
}

func (c *cluster) LocalNode() RaftNode {
	return c.raftNode
}

func (c *cluster) Config() ClusterConfig {
	return *c.config
}

func (c *cluster) Start() {
	go c.start()
}

func (c *cluster) start() {

	// Restore snapshot fro storage
	snap, err := c.config.Raft.Storage.Snapshot()
	if err != nil {
		c.config.Logger.Panicf("Error getting snapshot from raft storage: %v", err)
	}

	// Setup processor
	confState := snap.Metadata.ConfState
	snapi := snap.Metadata.Index
	appliedi := snapi

	// Start raft node
	go c.raftNode.Start()

	// Start processor loop
	var shouldstop bool
	for {
		select {
		case commit := <-c.raftNode.Commit():

			// apply snapshot
			if !raft.IsEmptySnap(commit.Snapshot) {
				if commit.Snapshot.Metadata.Index <= appliedi {
					c.config.Logger.Panicf("snapshot index [%d] should > appliedi[%d] + 1",
						commit.Snapshot.Metadata.Index, appliedi)
				}

				// Call Recoverer.Recover to recover Snapshot
				if err := c.config.Store.Recover(bytes.NewReader(commit.Snapshot.Data)); err != nil {
					c.config.Logger.Panicf("recovery store error: %v", err)
				}
				// s.cluster.Recover()

				// recover raft transport
				// s.r.transport.RemoveAllPeers()
				// for _, m := range s.cluster.Members() {
				// 	if m.ID == s.ID() {
				// 		continue
				// 	}
				// 	s.r.transport.AddPeer(m.ID, m.PeerURLs)
				// }

				appliedi = commit.Snapshot.Metadata.Index
				snapi = appliedi
				confState = commit.Snapshot.Metadata.ConfState
				c.config.Logger.Infof("recovered from incoming snapshot at index %d", snapi)
			}

			// apply entries
			if len(commit.CommittedEntries) != 0 {
				firsti := commit.CommittedEntries[0].Index
				if firsti > appliedi+1 {
					c.config.Logger.Panicf("first index of committed entry[%d] should <= appliedi[%d] + 1", firsti, appliedi)
				}
				var ents []raftpb.Entry
				if appliedi+1-firsti < uint64(len(commit.CommittedEntries)) {
					ents = commit.CommittedEntries[appliedi+1-firsti:]
				}
				if appliedi, shouldstop = c.apply(ents, &confState); shouldstop {
					go func() {
						time.Sleep(time.Second)
						select {
						case c.errorc <- fmt.Errorf("the member has been permanently removed from the cluster"):
						default:
						}
					}()
				}
			}

			// wait for the raft routine to finish the disk writes before triggering a
			// snapshot. or applied index might be greater than the last index in raft
			// storage, since the raft routine might be slower than apply routine.
			commit.Errorc <- nil

			// trigger snapshot
			if appliedi-snapi > c.config.SnapshotCount {
				c.config.Logger.Infof("start to snapshot (applied: %d, lastsnap: %d)", appliedi, snapi)
				c.raftNode.Snapshot(appliedi, confState)
				snapi = appliedi
			}
		case <-c.context.Done():
			return
		case <-c.errorc:
			return
		}
	}
}

// apply takes entries received from Raft (after it has been committed) and
// applies them to the current state of the EtcdServer.
// The given entries should not be empty.
func (c *cluster) apply(es []raftpb.Entry, confState *raftpb.ConfState) (uint64, bool) {
	var applied uint64
	var shouldstop bool
	var err error
	for _, e := range es {
		switch e.Type {
		case raftpb.EntryNormal:

			// raft state machine may generate noop entry when leader confirmation.
			// skip it in advance to avoid some potential bug in the future
			if len(e.Data) == 0 {
				// TODO: send update for cluster version
				break
			}

			// Process message
			if c.config.Applier != nil {
				c.config.Applier.Apply(e)
			}

		case raftpb.EntryConfChange:

			var cc raftpb.ConfChange
			pbutil.MustUnmarshal(&cc, e.Data)
			shouldstop, err = c.applyConfChange(cc, confState)
			c.raftNode.Trigger(cc.ID, err)
		default:
			c.config.Logger.Panicf("entry type should be either EntryNormal or EntryConfChange")
		}
		c.raftNode.Apply(e)
		applied = e.Index
	}
	return applied, shouldstop
}

// applyConfChange applies a ConfChange to the server. It is only
// invoked with a ConfChange that has already passed through Raft
func (c *cluster) applyConfChange(cc raftpb.ConfChange, confState *raftpb.ConfState) (bool, error) {
	*confState = *c.raftNode.ApplyConfChange(cc)

	// Create ClusterChangeEvent
	evt := ClusterChangeEvent{
		Member: NewMember(cc.NodeID, cc.Context),
	}

	// Set ClusterEventType
	switch cc.Type {
	case raftpb.ConfChangeAddNode:
		evt.Type = AddMember
	case raftpb.ConfChangeRemoveNode:
		evt.Type = RemoveMember
	case raftpb.ConfChangeUpdateNode:
		evt.Type = UpdateMember
	}

	// Emit ClusterChangeEvent
	c.Update(evt)
	return false, nil
}

// Update sends the ClusterChangeEvent to the Transporter
func (c *cluster) Update(evt ClusterChangeEvent) {
	c.transporter.Update(evt)
}

// Send sends raftpb.Messages to the Transporter
func (c *cluster) Send(ms []raftpb.Message) {
	c.transporter.Send(ms)
}

// Stop stops the cluster
func (c *cluster) Stop() {
	c.cancel()
}
