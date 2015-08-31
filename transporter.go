package raftor

import "golang.org/x/net/context"

// TransporterFactory creates a new Transporter which uses the given context to
// help manage its life-cycle.
type TransporterFactory func(context.Context) Transporter

// Transporter is an interface which is used to send raft.Messages to other peers
// in the cluster.
type Transporter interface {

	// Starter adds the Start() method to the Transporter so that the cluster
	// can manage when to start the Transporter.
	Starter

	// Updater adds the Update method to response to cluster change events.
	Updater

	// Sender adds the Send and is used to send messages to other peers in the cluster.
	Sender
}
