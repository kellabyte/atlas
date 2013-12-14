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
	NodesFromDataCenter(datacenter string) Nodes
	FirstNode() Node
	AddNamespace(namespace string) error
	Namespaces() map[string]Namespace
	Namespace(namespace string) (Namespace, error)
	NamespaceReplicasForKey(namespace string, key_hash uint64) Nodes
}

type ring struct {
	partitioner       Partitioner
	namespaces        map[string]Namespace
	nodes             Nodes
	nodesByDataCenter map[string]Nodes
}

func NewRing() Ring {
	ring := &ring{}
	ring.partitioner = NewMurMur3Partitioner()
	ring.namespaces = make(map[string]Namespace)
	ring.nodesByDataCenter = make(map[string]Nodes)
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

	var nodes Nodes
	nodes, _ = ring.nodesByDataCenter[node.DataCenter()]
	nodes = append(nodes, node)
	ring.nodesByDataCenter[node.DataCenter()] = nodes

	sort.Sort(ring.nodesByDataCenter[node.DataCenter()])
	sort.Sort(ring.nodes)
	return nil
}

func (ring *ring) Nodes() Nodes {
	return ring.nodes
}

func (ring *ring) NodesFromDataCenter(datacenter string) Nodes {
	return ring.nodesByDataCenter[datacenter]
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

func (ring *ring) NamespaceReplicasForKey(namespace string, key_hash uint64) Nodes {
	var replicas Nodes
	var ns Namespace
	var foundNamespace bool
	var replicaDataCenters map[string]uint
	var replicaDataCenterCounts map[string]uint
	var foundFirstReplica bool
	var firstReplicaSkipped bool

	replicaDataCenters = make(map[string]uint)
	replicaDataCenterCounts = make(map[string]uint)

	if ns, foundNamespace = ring.namespaces[namespace]; !foundNamespace {
		return nil
	}

	// Get all the data centers that include this namespace.
	for dc, replicationFactor := range ns.DataCenters() {
		replicaDataCenters[dc] = replicationFactor
		//fmt.Printf("ADDED DC: %s with RF: %d\n", dc, replicationFactor)
	}

	// Loop all nodes in the ring.
	for index, node := range ring.Nodes() {
		//fmt.Printf("DC: %s KEY: %d TOKEN: %d ", node.DataCenter(), key_hash, node.Token())
		// Make sure this node is in a destined datacenter with a replication factor.
		if replicationFactor, found := replicaDataCenters[node.DataCenter()]; found {
			// Find the owner for the key.
			if key_hash >= node.Token() && key_hash < ring.Nodes()[index + 1].Token() || firstReplicaSkipped {
				if replicationFactor > 0 {
					replicas = append(replicas, node)
					foundFirstReplica = true
					firstReplicaSkipped = false
					replicaDataCenterCounts[node.DataCenter()] = replicaDataCenterCounts[node.DataCenter()] + 1
					//fmt.Printf("ADDED FIRST")
				} else {
					firstReplicaSkipped = true
				}
			} else if foundFirstReplica == true &&
				replicaDataCenterCounts[node.DataCenter()] < replicaDataCenters[node.DataCenter()] {

				replicas = append(replicas, node)
				replicaDataCenterCounts[node.DataCenter()] = replicaDataCenterCounts[node.DataCenter()] + 1
				//fmt.Printf("ADDED")
			}
		}
		//fmt.Println()
	}

	// Check if we have all the required replicas.
	var loop bool = false
	for dc, replicationFactor := range replicaDataCenters {
		if rf, found := replicaDataCenterCounts[dc]; found && rf < replicationFactor {
			loop = true
		}
	}

	// Don't have enough replicas, loop to the beginning of the ring and keep seeking replicas.
	if loop == true {
		for _, node := range ring.Nodes() {
			//fmt.Printf("LOOP DC: %s KEY: %d TOKEN: %d ", node.DataCenter(), key_hash, node.Token())

			expectedReplicationFactor, _ := replicaDataCenters[node.DataCenter()]
			actualReplicationFactor, _ := replicaDataCenterCounts[node.DataCenter()]

			if actualReplicationFactor < expectedReplicationFactor {
				replicas = append(replicas, node)
				replicaDataCenterCounts[node.DataCenter()] = replicaDataCenterCounts[node.DataCenter()] + 1
				//fmt.Printf("ADDED")
			}

			var complete bool = true
			for dc, replicationFactor := range replicaDataCenters {
				rf, found := replicaDataCenterCounts[dc]
				if found && rf < replicationFactor {
					complete = false
				}
			}
			if complete == true {
				//fmt.Println()
				break
			}
			//fmt.Println()
		}
	}
	return replicas
}
