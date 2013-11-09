package topology

import (
	"errors"
	"sort"
)

var DataCenterNotFoundError = errors.New("Data center not found")
var NodeRequiredError = errors.New("Node required")
var DuplicateNodeError = errors.New("Node already exists")

type Ring interface {
	Topology
	FirstNode() Node
	AddNode(node Node) error
	Replicas(node Node, factor int) (Nodes, error)
	ReplicationFactor(center string) (int, error)
	SetReplicationFactor(center string, factor int)
}

type ring struct {
	dataCenters map[string]int
	nodes       Nodes
}

func NewRing() Ring {
	return &ring{
		dataCenters: make(map[string]int),
	}
}

func (r *ring) ReplicationFactor(center string) (int, error) {
	if _, exists := r.dataCenters[center]; !exists {
		return 0, DataCenterNotFoundError
	}
	return r.dataCenters[center], nil
}

func (r *ring) SetReplicationFactor(center string, factor int) {
	r.dataCenters[center] = factor
}

func (r *ring) FirstNode() Node {
	if len(r.nodes) == 0 {
		return nil
	}
	return r.nodes[0]
}

func (r *ring) AddNode(node Node) error {
	if node == nil {
		return NodeRequiredError
	} else if r.nodes.TokenExists(node.Token()) {
		return DuplicateNodeError
	}

	r.nodes = append(r.nodes, node)
	sort.Sort(r.nodes)
	return nil
}

func (r *ring) Replicas(node Node, factor int) (Nodes, error) {
	if node == nil {
		return nil, NodeRequiredError
	}

	// TODO? 

	return nil, nil
}
