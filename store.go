package raftor

import "io"

// Store provides an interface for saving a snapshot and also for restoring one.
type Store interface {
	Snapshot(io.Writer) error
	Recover(io.Reader) error
}
