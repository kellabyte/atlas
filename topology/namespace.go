package topology

type Namespace interface {
	Name() string
	AddDataCenter(dataCenter string, replicationFactor uint)
	DataCenters() map[string]uint
	ReplicationFactor(dataCenter string) uint
}

type namespace struct {
	name        string
	dataCenters map[string]uint
}

func NewNamespace(name string) Namespace {
	ns := &namespace{}
	ns.name = name
	ns.dataCenters = make(map[string]uint)
	return ns
}

func (ns *namespace) Name() string {
	return ns.name
}

func (ns *namespace) AddDataCenter(dataCenter string, replicationFactor uint) {
	ns.dataCenters[dataCenter] = replicationFactor
}

func (ns *namespace) DataCenters() map[string]uint {
	return ns.dataCenters
}

func (ns *namespace) ReplicationFactor(dataCenter string) uint {
	return ns.dataCenters[dataCenter]
}
