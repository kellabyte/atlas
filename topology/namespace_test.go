package topology

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNamespace_NewNamespace(t *testing.T) {
	ns := NewNamespace("foobar")

	assert.Equal(t, ns.Name(), "foobar")
}

func TestNamespace_AddDataCenter(t *testing.T) {
	ns := NewNamespace("foobar")
	ns.AddDataCenter("dc1", 3)

	assert.Equal(t, 1, len(ns.DataCenters()))
	assert.Equal(t, ns.ReplicationFactor("dc1"), uint(3))
}
