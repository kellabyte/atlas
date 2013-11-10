package topology

type Partitioner interface {
	GetMinimumToken() uint64
	GetMaximumToken() uint64
	GetToken(bytes []byte) uint64
}
