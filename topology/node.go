package topology

type Node struct {
	token      int64
	dataCenter string
}

type Nodes []*Node

// Len is the number of elements in the collection.
func (nodes Nodes) Len() int {
	return len(nodes)
}

// Less returns whether the element with index i should sort
// before the element with index j.
func (nodes Nodes) Less(i int, j int) bool {
	if nodes[i].token < nodes[j].token {
		return true
	}
	return false
}

// Swap swaps the elements with indexes i and j.
func (nodes Nodes) Swap(i int, j int) {
	nodes[i], nodes[j] = nodes[j], nodes[i]
}
