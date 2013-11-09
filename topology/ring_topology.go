package topology

import (
	//"github.com/spaolacci/murmur3"
	//"hash"
	//"io"
	"fmt"
	"sort"
)

type RingTopology struct {
	dataCenters map[string]int
	nodes       Nodes
}

func (ring *RingTopology) getReplicationFactorForDataCenter(dataCenter string) (int, error) {
	if ring.dataCenters == nil {
		return 0, fmt.Errorf("No datacenters added")
	}
	return ring.dataCenters[dataCenter], nil
}

func (ring *RingTopology) setDataCenter(dataCenter string, replicationFactor int) error {
	if ring.dataCenters == nil {
		ring.dataCenters = make(map[string]int)
	}
	ring.dataCenters[dataCenter] = replicationFactor
	return nil
}

func (ring *RingTopology) getFirstNode(node *Node) (*Node, error) {
	if ring == nil || ring.nodes == nil || len(ring.nodes) == 0 {
		return nil, fmt.Errorf("Error uninitialized RingTopology")
	}
	if node == nil {
		return nil, fmt.Errorf("Error uninitialized Node argument")
	}
	return ring.nodes[0], nil
}

func (ring *RingTopology) addNode(node *Node) error {
	if ring == nil {
		return fmt.Errorf("Error uninitialized RingTopology")
	}
	if node == nil {
		return fmt.Errorf("Error uninitialized Node argument")
	}

	for i := range ring.nodes {
		if ring.nodes[i].token == node.token {
			return fmt.Errorf("Error node with token %d already exists", node.token)
		}
	}

	ring.nodes = append(ring.nodes, node)
	sort.Sort(ring.nodes)
	return nil
}

func (ring *RingTopology) getReplicas(node *Node, replicationFactor int) (Nodes, error) {
	if ring == nil {
		return nil, fmt.Errorf("Error uninitialized RingTopology")
	}
	if node == nil {
		return nil, fmt.Errorf("Error uninitialized Node argument")
	}

	return nil, nil
}
