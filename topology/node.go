package topology

type Node interface {
	Token() int64
	DataCenter() string
}

type node struct {
	token      int64
	dataCenter string
}

func NewNode(token int64) Node {
	return &node{
		token: token,
	}
}

func (n *node) Token() int64 {
	return n.token
}

func (n *node) DataCenter() string {
	return n.dataCenter
}
