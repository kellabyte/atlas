package topology

type Nodes []Node

// TokenExists checks if the list contains a node with a given token.
func (s Nodes) TokenExists(token uint64) bool {
	for _, n := range s {
		if n != nil && n.Token() == token {
			return true
		}
	}
	return false
}

// Len is the number of elements in the collection.
func (s Nodes) Len() int {
	return len(s)
}

// Less returns whether the element with index i should sort
// before the element with index j.
func (s Nodes) Less(i int, j int) bool {
	return s[i].Token() < s[j].Token()
}

// Swap swaps the elements with indexes i and j.
func (s Nodes) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
}
