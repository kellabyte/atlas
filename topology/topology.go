package topology

type Topology interface {
	AddNode(node Node) error
	FirstNode() Node
	SetDataCenter(dataCenter string, replicationFactor int) error
	Replicas(node Node, replicationFactor int) (Nodes, error)
}
