package raftor

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blacklabeldata/raftor/pkg/murmur"
	"github.com/coreos/etcd/pkg/idutil"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"golang.org/x/net/context"
)

// RaftNode extends the etcd raft.Node.
type RaftNode interface {
	RemoteMessageHandler
	ProposalHandler
	Notifier
	Starter
	Applier
	raft.Node
	wait.Wait

	Snapshot(snapi uint64, confState raftpb.ConfState)

	// Commit
	Commit() chan Commit
}

// A RaftNodeFactory generates a RaftNode based on a parent context.Context and a Cluster.
type RaftNodeFactory func(context.Context, Cluster) RaftNode

// NewRaftNode creates a new RaftNode from the context.Context and the given Cluster.
func NewRaftNode(ctx context.Context, cfg ClusterConfig, tr Transporter) RaftNode {
	var mu sync.Mutex
	var peers []raft.Peer
	// cfg := cluster.Config()
	node := raft.StartNode(&cfg.Raft, peers)
	hash := murmur.Murmur3([]byte(cfg.LocalNodeName), murmur.M3Seed)

	return &raftNode{
		index:         0,
		term:          0,
		lead:          0,
		hash:          hash,
		lt:            time.Now(),
		Node:          node,
		parentContext: ctx,
		cfg:           cfg.Raft,
		numberOfCatchUpEntries: cfg.NumberOfCatchUpEntries,
		transporter:            tr,
		store:                  cfg.Store,
		raftStorage:            raft.NewMemoryStorage(),
		snapshotStorage:        cfg.SnapshotStorage,
		mu:                     mu,
		idgen:                  idutil.NewGenerator(uint8(hash), time.Now()),
		w:                      wait.New(),
		ticker:                 time.Tick(500 * time.Millisecond),

		// Channels
		notifyc: make(chan ClusterChangeEvent, 1),
		propc:   make(chan *Proposal, 1),
	}
}

type raftNode struct {
	// Cache of the latest raft index and raft term the server has seen.
	// These three unit64 fields must be the first elements to keep 64-bit
	// alignment for atomic access to the fields.
	index uint64
	term  uint64
	lead  uint64
	hash  uint32

	// last lead elected time
	lt time.Time

	//
	numberOfCatchUpEntries uint64

	// node raft.Node
	raft.Node

	applier         Applier
	transporter     Transporter
	store           Store
	parentContext   context.Context
	raftStorage     *raft.MemoryStorage
	snapshotStorage SnapshotStorage
	cfg             raft.Config
	mu              sync.Mutex

	idgen *idutil.Generator
	w     wait.Wait

	// utility
	ticker <-chan time.Time

	// Channels
	applyc  chan Commit
	notifyc chan ClusterChangeEvent
	propc   chan *Proposal
}

func (r *raftNode) Notify(evt ClusterChangeEvent) {
	r.notifyc <- evt
}

func (r *raftNode) Register(id uint64) <-chan interface{} {
	return r.w.Register(id)
}

func (r *raftNode) Trigger(id uint64, x interface{}) {
	r.w.Trigger(id, x)
}

func (r *raftNode) Apply(e raftpb.Entry) error {
	atomic.StoreUint64(&r.index, e.Index)
	atomic.StoreUint64(&r.term, e.Term)
	return nil
}

// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
// TODO: Ideally raftNode should get rid of the passed in server structure.
func (r *raftNode) Start() {
	r.applyc = make(chan Commit)
	defer close(r.applyc)

	var syncC <-chan time.Time
	for {
		select {

		// Process config change event
		case evt := <-r.notifyc:
			go r.handleConfChange(evt)

		// Process proposed message
		case m := <-r.propc:
			go r.proposeMessage(m)

		// Trigger raft tick
		case <-r.ticker:
			r.Tick()

		// Process raft commit
		case rd := <-r.Ready():
			if rd.SoftState != nil {

				// Update last time the leader was heard from with time.Now()
				// If there is not Leader or this node is the leader, don't update the timer.
				if lead := atomic.LoadUint64(&r.lead); rd.SoftState.Lead != raft.None && lead != rd.SoftState.Lead {
					r.mu.Lock()
					r.lt = time.Now()
					r.mu.Unlock()
				}

				// Save which node is the leader right now
				atomic.StoreUint64(&r.lead, rd.SoftState.Lead)

				// If this node is the leader, start the time ticker for heartbeats. If this node is not the leader, set the timer to nil.
				if rd.RaftState == raft.StateLeader {
					r.cfg.Logger.Infof("Elected Leader")
					syncC = time.Tick(500 * time.Millisecond)
				} else {
					syncC = nil
				}
			}

			// Create commit context
			ctx, cancel := context.WithCancel(r.parentContext)
			errorc := make(chan error)

			// Create commit
			commit := Commit{
				State: RaftState{
					CommitID:             rd.Commit,
					Vote:                 rd.Vote,
					Term:                 rd.Term,
					Lead:                 rd.Lead,
					LastLeadElectionTime: r.leadElectedTime(),
					RaftState:            rd.RaftState,
				},
				CommittedEntries: rd.CommittedEntries,
				Snapshot:         rd.Snapshot,
				Messages:         rd.Messages,
				Context:          ctx,
				Errorc:           errorc,
			}

			// Apply commit
			r.applyc <- commit

			// Save snapshot if there is one
			if !raft.IsEmptySnap(rd.Snapshot) {

				// Save snapshot to persistent storage
				if err := r.snapshotStorage.SaveSnap(rd.Snapshot); err != nil {
					r.cfg.Logger.Fatalf("Raft could not save snapshot", err)
				}

				// Apply snapshot to in-memory storage
				r.raftStorage.ApplySnapshot(rd.Snapshot)
				r.cfg.Logger.Infof("Raft applied incoming snapshot at index %d", rd.Snapshot.Metadata.Index)
			}

			// Save state and entries to persistent storage
			if err := r.snapshotStorage.Save(rd.HardState, rd.Entries); err != nil {
				r.cfg.Logger.Fatalf("Raft could not save snapshot: %v", err)
			}

			// Append entries to im-memory storage
			r.raftStorage.Append(rd.Entries)

			// Send to other nodes
			r.transporter.Send(rd.Messages)

			select {

			// Wait for commit to be applied
			case <-ctx.Done():

			// Or server stopped
			case <-r.parentContext.Done():
				r.cfg.Logger.Infof("Server stopped.")

				// Cancel current commit then return
				cancel()
				return
			}

			// Advance raft
			r.Advance()

		// This is only called if this local node is the leader
		case <-syncC:
			// log.Printf("[raft] Sync")
			// r.sync(r.s.cfg.ReqTimeout())

		case <-r.parentContext.Done():
			r.cfg.Logger.Infof("Server stopped.")
			return
		}
	}
}

func (r *raftNode) Commit() chan Commit {
	return r.applyc
}

func (r *raftNode) handleConfChange(evt ClusterChangeEvent) {
	// // Don't add yourself to the cluster
	// if evt.Member.ID() == uint64(r.hash) {
	// 	return
	// }

	// Create ConfChange message
	cc := raftpb.ConfChange{
		NodeID:  evt.Member.ID(),
		Context: evt.Member.Meta(),
	}

	// Set type for ConfChange
	switch evt.Type {
	case AddMember:
		cc.Type = raftpb.ConfChangeAddNode
	case RemoveMember:
		cc.Type = raftpb.ConfChangeAddNode
	case UpdateMember:
		cc.Type = raftpb.ConfChangeAddNode
	default:
		r.cfg.Logger.Errorf("Unknown ClusterEventType: %s", evt.Type)
		return
	}

	// Propose change and log possible error. Retry once on failure.
	if err := r.proposeConfChange(cc); err != nil {
		r.cfg.Logger.Errorf("Error proposing config change: %s", err)
		r.cfg.Logger.Infof("Trying again: %#v", evt)

		// Propose retry and log error
		if err := r.proposeConfChange(cc); err != nil {
			r.cfg.Logger.Errorf("Error retrying config change: %s", err)
		}
	}
}

// proposeConfChange sends a configuration change through consensus and
// then waits for it to be applied to the server. It
// will block until the change is performed or there is an error.
func (r *raftNode) proposeConfChange(cc raftpb.ConfChange) error {

	// Set rangom change id
	cc.ID = r.idgen.Next()

	// Register wait channel
	ch := r.w.Register(cc.ID)

	// Record ProposeConfChange start
	start := time.Now()

	// Execute ProposeConfChange and trigger done channel
	go func() {
		err := r.ProposeConfChange(r.parentContext, cc)
		r.w.Trigger(cc.ID, err)
	}()

	// Wait for request to finish or server is stopped
	select {

	// Request completed
	case x := <-ch:

		// If error, return error
		if err, ok := x.(error); ok {
			return err
		}

		// If returned object is not an error, it should be nil. Otherwise return error.
		if x != nil {
			return fmt.Errorf("return type should always be error")
		}

		// No error occurred and request succeeded
		return nil

	// Parent context was collapsed (i.e. server was stopped)
	case <-r.parentContext.Done():

		// Close wait channel
		r.w.Trigger(cc.ID, nil) // GC wait

		// Parse context error
		return r.parseProposeCtxErr(r.parentContext.Err(), start)
	}
}

// parseProposeCtxErr returns an appropriate error based on the result of the context close event.
func (r *raftNode) parseProposeCtxErr(err error, start time.Time) error {
	switch err {

	// Context was cancelled
	case context.Canceled:
		return ErrCanceled

	// Context deadline was exceeded
	case context.DeadlineExceeded:

		// Determine if config change was due to leader failure and election
		curLeadElected := r.leadElectedTime()
		prevLeadLost := curLeadElected.Add(-2 * time.Duration(r.cfg.ElectionTick) * time.Duration(1) * time.Millisecond)
		if start.After(prevLeadLost) && start.Before(curLeadElected) {
			return ErrTimeoutDueToLeaderFail
		}
		return ErrTimeout

	// Otherwise return the error
	default:
		return err
	}
}

// leadElectedTime returns the timestamp of the last received message
func (r *raftNode) leadElectedTime() (lt time.Time) {
	r.mu.Lock()
	lt = r.lt
	r.mu.Unlock()
	return
}

func (r *raftNode) Context() context.Context {
	return r.parentContext
}

func (r *raftNode) HandleRemoteMessage(msg raftpb.Message) {
	r.Step(r.parentContext, msg)
}

func (r *raftNode) HandleProposal(p *Proposal) {
	r.propc <- p
}

// proposeMessage sends a configuration change through consensus and
// then waits for it to be applied to the server. It
// will block until the change is performed or there is an error.
func (r *raftNode) proposeMessage(p *Proposal) {

	// get rangom message id
	id := r.idgen.Next()

	// Register wait channel
	ch := r.w.Register(id)

	// Record ProposeConfChange start
	start := time.Now()

	// Execute Propose and trigger done channel
	go func() {
		err := r.Propose(p.Context, p.Data)
		r.w.Trigger(id, err)
	}()

	// Wait for request to finish or server is stopped
	select {

	// Request completed
	case x := <-ch:

		// If error, return error
		if err, ok := x.(error); ok {
			p.Errorc <- err
		}

		// If returned object is not an error, it should be nil. Otherwise return error.
		if x != nil {
			p.Errorc <- fmt.Errorf("return type should always be an error, not '%T'", x)
		}

		// No error occurred and request succeeded
		p.Errorc <- nil

	// Parent context was collapsed (i.e. server was stopped)
	case <-r.parentContext.Done():

		// Close wait channel
		r.w.Trigger(id, nil) // GC wait

		// Parse context error
		p.Errorc <- r.parseProposeCtxErr(r.parentContext.Err(), start)

	// Message context was collapsed
	case <-p.Context.Done():

		// Close wait channel
		r.w.Trigger(id, nil) // GC wait
	}
}

func (r *raftNode) Snapshot(snapi uint64, confState raftpb.ConfState) {
	go r.snapshot(snapi, confState)
}

func (r *raftNode) snapshot(snapi uint64, confState raftpb.ConfState) {
	var writer bytes.Buffer
	err := r.store.Snapshot(&writer)

	// TODO: current store will never fail to do a snapshot
	// what should we do if the store might fail?
	if err != nil {
		r.cfg.Logger.Errorf("store save should never fail: %v", err)
		return
	}
	snap, err := r.raftStorage.CreateSnapshot(snapi, &confState, writer.Bytes())
	if err != nil {
		// the snapshot was done asynchronously with the progress of raft.
		// raft might have already got a newer snapshot.
		if err == raft.ErrSnapOutOfDate {
			return
		}
		r.cfg.Logger.Errorf("unexpected create snapshot error %v", err)
		return
	}

	if err := r.snapshotStorage.SaveSnap(snap); err != nil {
		r.cfg.Logger.Errorf("save snapshot error: %v", err)
		return
	}
	r.cfg.Logger.Infof("saved snapshot at index %d", snap.Metadata.Index)

	// keep some in memory log entries for slow followers.
	compacti := uint64(1)
	if snapi > r.numberOfCatchUpEntries {
		compacti = snapi - r.numberOfCatchUpEntries
	}
	err = r.raftStorage.Compact(compacti)
	if err != nil {
		// the compaction was done asynchronously with the progress of raft.
		// raft log might already been compact.
		if err == raft.ErrCompacted {
			return
		}
		r.cfg.Logger.Panicf("unexpected compaction error %v", err)
	}
	r.cfg.Logger.Infof("compacted raft log at %d", compacti)
}

// // sync proposes a SYNC request and is non-blocking.
// // This makes no guarantee that the request will be proposed or performed.
// // The request will be cancelled after the given timeout.
// func (r *raftNode) sync(timeout time.Duration) {
// 	ctx, cancel := context.WithTimeout(r.parentContext, timeout)
// 	req := raftpb.Request{
// 		Method: "SYNC",
// 		ID:     r.idgen.Next(),
// 		Time:   time.Now().UnixNano(),
// 	}
// 	data := pbutil.MustMarshal(&req)
// 	// There is no promise that node has leader when do SYNC request,
// 	// so it uses goroutine to propose.
// 	go func() {
// 		r.Propose(ctx, data)
// 		cancel()
// 	}()
// }

// // send relays all the messages to the transport
// func (r *raftNode) send(ms []raftpb.Message) {
// 	for i := range ms {

// 		// The etcd implementation sets the msg.To field to 0 is the node has been removed.
// 		// I'm currently not sure if the transport is supposed to ignore that message or not based on the To field.
// 		if _, ok := r.removed[ms[i].To]; ok {
// 			ms[i].To = 0
// 		}
// 	}

// 	// Send messages over transport
// 	r.cluster.Send(ms)
// }
