package topology

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRingTopology_FirstNode_NoNodes(t *testing.T) {
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
	assert.Equal(t, len(r.(*ring).nodes), 1) // TODO: Testing private state feels wrong.
}

func TestRingTopology_AddDuplicate(t *testing.T) {
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
	assert.Equal(t, nodes[0].Token(), uint64(0))
	assert.Equal(t, nodes[1].Token(), uint64(1))
	assert.Equal(t, nodes[2].Token(), uint64(2))
}

func TestRingTopology_ReplicationFactorForDataCenter(t *testing.T) {
	r := NewRing()
	r.SetDataCenter("dc1", 2)
	r.SetDataCenter("dc1", 3)

	factor, err := r.ReplicationFactorForDataCenter("dc1")
	assert.NoError(t, err)
	assert.Equal(t, factor, 3)
}
