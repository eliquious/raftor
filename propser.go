package raftor

import "golang.org/x/net/context"

// Proposal is simply an encapsulation of a context.Context,
// message content and an error channel and is used to send
// data to the Raft cluster.
type Proposal struct {

	// The Context should be handled by the creator of the Message. If the
	// Context is closed before the processing has completed, an error channel will
	// not be closed by the ProposalHandler and should be closed by the caller. If
	// the Errorc is not closed when the Context has completed, the error channel
	// will remain open and will result in a memory leak.
	Context context.Context

	// Data is the content of the message being sent to the cluster.
	Data []byte

	// Errorc is the channel over which errors will be sent if the Proposal
	// was not processed successfully. This channel should be closed by the
	// creator of the Proposal. If it is not closed after the Context has
	// completed, the channel will fail to be garbage collected.
	Errorc chan error
}

// ProposalHandler proposes messages to the Raft cluster.
type ProposalHandler interface {

	// HandleProposal is called when a message is being proposed to the Raft cluster.
	// This method should respond in a timely manner and allow the caller to handle
	// the response via the Proposal's Context and Error channel.
	HandleProposal(*Proposal)
}
