package topology

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMurMur3Partitioner_getMinimumToken(t *testing.T) {
	partitioner := NewMurMur3Partitioner()
	expected := uint64(0)
	actual := partitioner.GetMinimumToken()

	assert.Equal(t, expected, actual)
}

func TestMurMur3Partitioner_getMaximumToken(t *testing.T) {
	partitioner := NewMurMur3Partitioner()
	expected := uint64(18446744073709551615)
	actual := partitioner.GetMaximumToken()

	assert.Equal(t, expected, actual)
}

func TestMurMur3Partitioner_getToken(t *testing.T) {
	partitioner := NewMurMur3Partitioner()
	expected := uint64(0x8b95f808840725c6)
	actual := partitioner.GetToken([]byte("hello, world"))

	assert.Equal(t, expected, actual)
}
