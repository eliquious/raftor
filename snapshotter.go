package raftor

type Snapshotter interface {
	Snapshot() ([]byte, error)
}
