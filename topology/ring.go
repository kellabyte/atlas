package topology

import (
	"errors"
	"sort"
)

var DataCenterNotFoundError = errors.New("Data center not found")
var NamespaceNotFoundError = errors.New("Namespace not found")
var NodeRequiredError = errors.New("Node required")
var DuplicateNodeError = errors.New("Node already exists")
var DuplicateNamespaceError = errors.New("Namespace already exists")

type Ring interface {
	Partitioner() Partitioner
	AddNode(node Node) error
	Nodes() Nodes
	FirstNode() Node
	AddNamespace(namespace string) error
	Namespaces() map[string]Namespace
	Namespace(namespace string) (Namespace, error)
}

type ring struct {
	partitioner Partitioner
	namespaces  map[string]Namespace
	nodes       Nodes
}

func NewRing() Ring {
	ring := &ring{}
	ring.partitioner = NewMurMur3Partitioner()
	ring.namespaces = make(map[string]Namespace)
	return ring
}

func (ring *ring) Partitioner() Partitioner {
	return ring.partitioner
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

func (ring *ring) Nodes() Nodes {
	return ring.nodes
}

func (ring *ring) FirstNode() Node {
	if len(ring.nodes) == 0 {
		return nil
	}
	return ring.nodes[0]
}

func (ring *ring) AddNamespace(namespace string) error {
	if _, ok := ring.namespaces[namespace]; ok {
		return DuplicateNamespaceError
	}
	ns := NewNamespace(namespace)
	ring.namespaces[namespace] = ns
	return nil
}

func (ring *ring) Namespaces() map[string]Namespace {
	return ring.namespaces
}

func (ring *ring) Namespace(namespace string) (Namespace, error) {
	if ns, ok := ring.namespaces[namespace]; ok {
		return ns, nil
	}
	return nil, NamespaceNotFoundError
}
