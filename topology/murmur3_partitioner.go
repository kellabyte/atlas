package topology

import (
	"github.com/spaolacci/murmur3"
)

type murMur3Partitioner struct {
}

func NewMurMur3Partitioner() Partitioner {
	partitioner := &murMur3Partitioner{}
	return partitioner
}

func (partitioner *murMur3Partitioner) GetMinimumToken() uint64 {
	return 0
}

func (partitioner *murMur3Partitioner) GetMaximumToken() uint64 {
	return ^uint64(0)
}

func (partitioner *murMur3Partitioner) GetToken(bytes []byte) uint64 {
	return murmur3.Sum64(bytes)
}
