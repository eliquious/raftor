package raftor

type Recoverer interface {
	Recover([]byte) error
}
