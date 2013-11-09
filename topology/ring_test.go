package topology

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRingTopology_FirstNode_NilNode(t *testing.T) {
	r := NewRing()
	node := r.FirstNode()
	assert.Nil(t, node)
}

func TestRingTopology_AddNode_NilNode(t *testing.T) {
	r := NewRing()
	err := r.AddNode(nil)
	assert.Equal(t, err, NodeRequiredError)
}

func TestRingTopology_AddNode(t *testing.T) {
	r := NewRing()
	node := NewNode(0)
	err := r.AddNode(node)
	assert.NoError(t, err)
	assert.Equal(t, len(r.(*ring).nodes), 1)
}

func TestRingTopology_AddNode_Duplicate(t *testing.T) {
	r := NewRing()
	node := NewNode(0)
	err := r.AddNode(node)
	assert.NoError(t, err)

	// Expect an error for the duplicate add.
	err = r.AddNode(node)
	assert.Equal(t, len(r.(*ring).nodes), 1)
}

func TestRingTopology_AddNode_Sorted(t *testing.T) {
	r := NewRing()
	r.AddNode(NewNode(2))
	r.AddNode(NewNode(1))
	r.AddNode(NewNode(0))

	nodes := r.(*ring).nodes
	assert.Equal(t, len(nodes), 3)
	assert.Equal(t, nodes[0].Token(), 0)
	assert.Equal(t, nodes[1].Token(), 1)
	assert.Equal(t, nodes[2].Token(), 2)
}

func TestRingTopology_ReplicationFactor(t *testing.T) {
	r := NewRing()
	r.SetReplicationFactor("dc1", 2)
	r.SetReplicationFactor("dc1", 3)

	factor, err := r.ReplicationFactor("dc1")
	assert.NoError(t, err)
	assert.Equal(t, factor, 3)
}
