package topology

type Node interface {
	Token() uint64
	DataCenter() string
}

type node struct {
	token      uint64
	dataCenter string
}

func NewNode(token uint64) Node {
	return &node{
		token: token,
	}
}

func (n *node) Token() uint64 {
	return n.token
}

func (n *node) DataCenter() string {
	return n.dataCenter
}
