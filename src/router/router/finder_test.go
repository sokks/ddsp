package router

import (
	"testing"

	"storage"
)

type FakeHasher struct {
	t      *testing.T
	hashes map[storage.ServiceAddr]uint64
}

func (h FakeHasher) Hash(k storage.RecordID, node storage.ServiceAddr) uint64 {
	hash, ok := h.hashes[node]
	if !ok {
		h.t.Fatalf("Unknown node %v", node)
	}

	return hash
}

func TestNodesFind(t *testing.T) {
	hrw := NewNodesFinder(FakeHasher{
		t: t,
		hashes: map[storage.ServiceAddr]uint64{
			"node1": 1,
			"node2": 2,
			"node3": 3,
			"node4": 4,
			"node5": 5,
			"node6": 6,
		},
	})
	nodes := []storage.ServiceAddr{"node1", "node2", "node3", "node4", "node5", "node6"}
	got := hrw.NodesFind(1, nodes)
	if !equalNodes(got, nodes[3:]) {
		t.Errorf("NodesFind() wrong nodes, got %v, want %v", got, nodes[3:])
	}
}

func TestNodes_SameHashes(t *testing.T) {
	hrw := NewNodesFinder(FakeHasher{
		t: t,
		hashes: map[storage.ServiceAddr]uint64{
			"node1": 1,
			"node2": 2,
			"node3": 3,
			"node4": 5,
			"node5": 5,
			"node6": 5,
		},
	})
	nodes := []storage.ServiceAddr{"node1", "node2", "node3", "node4", "node5", "node6"}
	got := hrw.NodesFind(1, nodes)
	if !equalNodes(got, nodes[3:]) {
		t.Errorf("NodesFind() wrong nodes, got %v, want %v", got, nodes[3:])
	}
}
