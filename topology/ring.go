package topology

import (
	"errors"
	"sort"
)

var DataCenterNotFoundError = errors.New("Data center not found")
var NodeRequiredError = errors.New("Node required")
var DuplicateNodeError = errors.New("Node already exists")

type Ring interface {
	Partitioner() Partitioner
	DataCenters() map[string]int
	Nodes() Nodes
	ReplicationFactorForDataCenter(dataCenter string) (int, error)
	SetDataCenter(dataCenter string, replicationFactor int) error
	FirstNode() Node
	AddNode(node Node) error
	Replicas(node Node, replicationFactor int) (Nodes, error)
}

type ring struct {
	partitioner Partitioner
	dataCenters map[string]int
	nodes       Nodes
}

func NewRing() Ring {
	ring := &ring{}
	ring.partitioner = NewMurMur3Partitioner()
	return ring
}

func (ring *ring) Partitioner() Partitioner {
	return ring.partitioner
}

func (ring *ring) DataCenters() map[string]int {
	return ring.dataCenters
}

func (ring *ring) Nodes() Nodes {
	return ring.nodes
}

func (ring *ring) ReplicationFactorForDataCenter(dataCenter string) (int, error) {
	if _, exists := ring.dataCenters[dataCenter]; !exists {
		return 0, DataCenterNotFoundError
	}
	return ring.dataCenters[dataCenter], nil
}

func (ring *ring) SetDataCenter(dataCenter string, replicationFactor int) error {
	if ring.dataCenters == nil {
		ring.dataCenters = make(map[string]int)
	}
	ring.dataCenters[dataCenter] = replicationFactor
	return nil
}

func (ring *ring) AddNode(node Node) error {
	if node == nil {
		return NodeRequiredError
	} else if ring.Nodes().TokenExists(node.Token()) {
		return DuplicateNodeError
	}

	ring.nodes = append(ring.nodes, node)
	sort.Sort(ring.nodes)
	return nil
}

func (ring *ring) FirstNode() Node {
	if len(ring.nodes) == 0 {
		return nil
	}
	return ring.nodes[0]
}

func (ring *ring) Replicas(node Node, replicationFactor int) (Nodes, error) {
	if node == nil {
		return nil, NodeRequiredError
	}
	// TODO

	return nil, nil
}
