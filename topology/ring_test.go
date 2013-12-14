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
	node := NewNode(0, "dc1")
	err := r.AddNode(node)
	assert.NoError(t, err)
	assert.Equal(t, len(r.(*ring).nodes), 1) // TODO: Testing private state feels wrong.
}

func TestRingTopology_AddNode_ToDataCenter(t *testing.T) {
	r := NewRing()
	node1 := NewNode(0, "dc1")
	node2 := NewNode(2, "dc1")
	node3 := NewNode(1, "dc2")
	err1 := r.AddNode(node1)
	err2 := r.AddNode(node2)
	err3 := r.AddNode(node3)

	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NoError(t, err3)
	assert.Equal(t, len(r.(*ring).nodes), 3) // TODO: Testing private state feels wrong.
	assert.Equal(t, len(r.(*ring).NodesFromDataCenter("dc1")), 2)
	assert.Equal(t, len(r.(*ring).NodesFromDataCenter("dc2")), 1)
}

func TestRingTopology_AddDuplicateNode(t *testing.T) {
	r := NewRing()
	node := NewNode(0, "dc1")
	err := r.AddNode(node)
	assert.NoError(t, err)

	// Expect an error for the duplicate add.
	err = r.AddNode(node)
	assert.Equal(t, len(r.(*ring).nodes), 1)
}

func TestRingTopology_AddNode_Sorted(t *testing.T) {
	r := NewRing()
	r.AddNode(NewNode(2, "dc1"))
	r.AddNode(NewNode(1, "dc1"))
	r.AddNode(NewNode(0, "dc1"))

	nodes := r.(*ring).nodes
	assert.Equal(t, len(nodes), 3)
	assert.Equal(t, nodes[0].Token(), uint64(0))
	assert.Equal(t, nodes[1].Token(), uint64(1))
	assert.Equal(t, nodes[2].Token(), uint64(2))
}

func TestRingTopology_AddNamespace(t *testing.T) {
	r := NewRing()
	err := r.AddNamespace("foobar")

	assert.NoError(t, err)
	assert.Equal(t, len(r.Namespaces()), 1)
}

func TestRingTopology_AddDuplicateNamespace(t *testing.T) {
	r := NewRing()
	r.AddNamespace("foobar")
	err := r.AddNamespace("foobar")

	assert.Error(t, err)
	assert.Equal(t, len(r.Namespaces()), 1)
}

func TestRingTopology_FindReplicas_ForKey(t *testing.T) {
	r := NewRing()

	r.AddNamespace("test_namespace")
	ns, _ := r.Namespace("test_namespace")
	ns.AddDataCenter("dc1", 2)
	ns.AddDataCenter("dc2", 2)
	ns.AddDataCenter("dc3", 2)

	node1 := NewNode(0, "dc1")
	node2 := NewNode(1, "dc2")
	node3 := NewNode(2, "dc3")
	node4 := NewNode(3, "dc1")
	node5 := NewNode(4, "dc2")
	node6 := NewNode(5, "dc3")
	node7 := NewNode(6, "dc1")
	node8 := NewNode(7, "dc2")
	node9 := NewNode(8, "dc3")
	r.AddNode(node1)
	r.AddNode(node2)
	r.AddNode(node3)
	r.AddNode(node4)
	r.AddNode(node5)
	r.AddNode(node6)
	r.AddNode(node7)
	r.AddNode(node8)
	r.AddNode(node9)

	replicas := r.NamespaceReplicasForKey("test_namespace", 0)
	assert.Equal(t, replicas.Len(), 6)
	assert.Equal(t, replicas[0], node1)
	assert.Equal(t, replicas[1], node2)
	assert.Equal(t, replicas[2], node3)
	assert.Equal(t, replicas[3], node4)
	assert.Equal(t, replicas[4], node5)
	assert.Equal(t, replicas[5], node6)
}

func TestRingTopology_FindLoopedReplicas_ForKey(t *testing.T) {
	r := NewRing()

	r.AddNamespace("test_namespace")
	ns, _ := r.Namespace("test_namespace")
	ns.AddDataCenter("dc1", 2)
	ns.AddDataCenter("dc2", 2)
	ns.AddDataCenter("dc3", 2)

	node1 := NewNode(0, "dc1")
	node2 := NewNode(1, "dc2")
	node3 := NewNode(2, "dc3")
	node4 := NewNode(3, "dc1")
	node5 := NewNode(4, "dc2")
	node6 := NewNode(5, "dc3")
	node7 := NewNode(6, "dc1")
	node8 := NewNode(7, "dc2")
	node9 := NewNode(8, "dc3")
	r.AddNode(node1)
	r.AddNode(node2)
	r.AddNode(node3)
	r.AddNode(node4)
	r.AddNode(node5)
	r.AddNode(node6)
	r.AddNode(node7)
	r.AddNode(node8)
	r.AddNode(node9)

	replicas := r.NamespaceReplicasForKey("test_namespace", 4)
	assert.Equal(t, replicas.Len(), 6)
	assert.Equal(t, replicas[0], node5)
	assert.Equal(t, replicas[1], node6)
	assert.Equal(t, replicas[2], node7)
	assert.Equal(t, replicas[3], node8)
	assert.Equal(t, replicas[4], node9)
	assert.Equal(t, replicas[5], node1)
}

func TestRingTopology_FindDifferingReplicas_ForKey(t *testing.T) {
	r := NewRing()

	r.AddNamespace("test_namespace")
	ns, _ := r.Namespace("test_namespace")
	ns.AddDataCenter("dc1", 2)
	ns.AddDataCenter("dc2", 1)
	ns.AddDataCenter("dc3", 3)

	node1 := NewNode(0, "dc1")
	node2 := NewNode(1, "dc2")
	node3 := NewNode(2, "dc3")
	node4 := NewNode(3, "dc1")
	node5 := NewNode(4, "dc2")
	node6 := NewNode(5, "dc3")
	node7 := NewNode(6, "dc1")
	node8 := NewNode(7, "dc2")
	node9 := NewNode(8, "dc3")
	r.AddNode(node1)
	r.AddNode(node2)
	r.AddNode(node3)
	r.AddNode(node4)
	r.AddNode(node5)
	r.AddNode(node6)
	r.AddNode(node7)
	r.AddNode(node8)
	r.AddNode(node9)

	replicas := r.NamespaceReplicasForKey("test_namespace", 4)
	assert.Equal(t, replicas.Len(), 6)
	assert.Equal(t, replicas[0], node5)
	assert.Equal(t, replicas[1], node6)
	assert.Equal(t, replicas[2], node7)
	assert.Equal(t, replicas[3], node9)
	assert.Equal(t, replicas[4], node1)
	assert.Equal(t, replicas[5], node3)
}

func TestRingTopology_FindDifferingReplicasEmptyDC_ForKey(t *testing.T) {
	r := NewRing()

	r.AddNamespace("test_namespace")
	ns, _ := r.Namespace("test_namespace")
	ns.AddDataCenter("dc1", 2)
	ns.AddDataCenter("dc2", 0)
	ns.AddDataCenter("dc3", 3)

	node1 := NewNode(0, "dc1")
	node2 := NewNode(1, "dc2")
	node3 := NewNode(2, "dc3")
	node4 := NewNode(3, "dc1")
	node5 := NewNode(4, "dc2")
	node6 := NewNode(5, "dc3")
	node7 := NewNode(6, "dc1")
	node8 := NewNode(7, "dc2")
	node9 := NewNode(8, "dc3")
	r.AddNode(node1)
	r.AddNode(node2)
	r.AddNode(node3)
	r.AddNode(node4)
	r.AddNode(node5)
	r.AddNode(node6)
	r.AddNode(node7)
	r.AddNode(node8)
	r.AddNode(node9)

	replicas := r.NamespaceReplicasForKey("test_namespace", 4)
	assert.Equal(t, replicas.Len(), 5)
	assert.Equal(t, replicas[0], node6)
	assert.Equal(t, replicas[1], node7)
	assert.Equal(t, replicas[2], node9)
	assert.Equal(t, replicas[3], node1)
	assert.Equal(t, replicas[4], node3)
}
