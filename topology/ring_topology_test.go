package topology

import (
	"testing"
)

func TestRingTopology_getFirstNode_NilNode(t *testing.T) {
	ring := &RingTopology{}

	if _, err := ring.getFirstNode(nil); err == nil {
		t.Fatalf("error should be thrown but wasn't")
	}
}

func TestRingTopology_getFirstNode_NoNodes(t *testing.T) {
	ring := &RingTopology{}
	node := &Node{
		token: 0,
	}

	if _, err := ring.getFirstNode(node); err == nil {
		t.Fatalf("error should be thrown but wasn't")
	}
}

func TestRingTopology_addNode_NilNode(t *testing.T) {
	ring := &RingTopology{}

	if err := ring.addNode(nil); err == nil {
		t.Fatalf("error should be thrown but wasn't")
	}
}

func TestRingTopology_addNode(t *testing.T) {
	ring := &RingTopology{}
	node := &Node{
		token: 0,
	}

	if err := ring.addNode(node); err != nil {
		t.Fatalf("%s", err)
	}

	if len(ring.nodes) != 1 {
		t.Fatalf("Ring didn't add the node specified")
	}
}

func TestRingTopology_addDuplicateNode(t *testing.T) {
	ring := &RingTopology{}
	node := &Node{
		token: 0,
	}

	if err := ring.addNode(node); err != nil {
		t.Fatalf("%s", err)
	}

	// Expect an error for the duplicate add.
	if err := ring.addNode(node); err == nil {
		t.Fatalf("%s", err)
	}

	if len(ring.nodes) != 1 {
		t.Fatalf("Ring didn't add the node specified")
	}
}

func TestRingTopology_addNode_Sorted(t *testing.T) {
	ring := &RingTopology{}
	node1 := &Node{
		token: 2,
	}
	node2 := &Node{
		token: 1,
	}
	node3 := &Node{
		token: 0,
	}

	if err := ring.addNode(node1); err != nil {
		t.Fatalf("%s", err)
	}
	if err := ring.addNode(node2); err != nil {
		t.Fatalf("%s", err)
	}
	if err := ring.addNode(node3); err != nil {
		t.Fatalf("%s", err)
	}

	if len(ring.nodes) != 3 {
		t.Fatalf("Ring didn't add the node specified")
	}

	if ring.nodes[0].token != 0 || ring.nodes[1].token != 1 || ring.nodes[2].token != 2 {
		t.Fatalf("Ring nodes not sorted")
	}
}

func TestRingTopology_getReplicationFactorForDataCenter(t *testing.T) {
	ring := &RingTopology{}
	ring.setDataCenter("dc1", 2)
	ring.setDataCenter("dc1", 3)

	var rf int
	var err error

	if rf, err = ring.getReplicationFactorForDataCenter("dc1"); err != nil {
		t.Fatalf("Error getting replication factor for data center")
	}
	if rf != 3 {
		t.Fatalf("Incorrect replication factor. Expected: %d Actual: %d", 3, rf)
	}

}
