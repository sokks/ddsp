package router

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"

	"storage"
)

var cfg = Config{
	Addr:  "router",
	Nodes: []storage.ServiceAddr{"node1", "node2", "node3"},
	NodesFinder: NewNodesFinder(FakeHasher{
		hashes: map[storage.ServiceAddr]uint64{
			"node1": 1,
			"node2": 2,
			"node3": 3,
		}}),

	ForgetTimeout: 100 * time.Millisecond,
}

func registerNodes(t *testing.T, r *Router, nodes []storage.ServiceAddr, forget time.Duration) {
	time.Sleep(forget)
	for _, node := range nodes {
		if err := r.Heartbeat(node); err != nil {
			t.Fatalf("Hearbeat() error: %v", err)
		}
	}
}

func equalNodes(a []storage.ServiceAddr, b []storage.ServiceAddr) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Slice(a, func(i, j int) bool {
		return a[i] < a[j]
	})
	sort.Slice(b, func(i, j int) bool {
		return b[i] < b[j]
	})
	return reflect.DeepEqual(a, b)
}

func TestNew(t *testing.T) {
	c := cfg
	c.Nodes = c.Nodes[:2]
	if _, err := New(c); err != storage.ErrNotEnoughDaemons {
		t.Errorf("New expected error %v, got %v", storage.ErrNotEnoughDaemons, err)
	}
}

func TestList(t *testing.T) {
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	nodes := r.List()
	if !equalNodes(nodes, cfg.Nodes) {
		t.Errorf("Wrong list of nodes got %v, want %v", nodes, cfg.Nodes)
	}

	time.Sleep(cfg.ForgetTimeout)

	nodes = r.List()
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i] < nodes[j]
	})

	if !equalNodes(nodes, cfg.Nodes) {
		t.Errorf("Wrong list of nodes got %v, want %v", nodes, cfg.Nodes)
	}
}

func TestHeartbeat(t *testing.T) {
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if err := r.Heartbeat("unknown"); err != storage.ErrUnknownDaemon {
		t.Errorf("Hearbeat() got %v, exptected error %v", err, storage.ErrUnknownDaemon)
	}

	if err := r.Heartbeat(cfg.Nodes[0]); err != nil {
		t.Errorf("Heartbeat() error: %v", err)
	}
}

func TestParallelOps(t *testing.T) {
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	go func() {
		for {
			runtime.Gosched()
			nodes := r.List()
			if !equalNodes(nodes, cfg.Nodes) {
				t.Fatalf("Wrong list of nodes got %v, want %v", nodes, cfg.Nodes)
			}
		}
	}()

	go func() {
		for {
			runtime.Gosched()
			for _, node := range cfg.Nodes {
				if err := r.Heartbeat(node); err != nil {
					t.Fatalf("Hearbeat(%v) error: %v", node, err)
				}
			}
		}
	}()

	go func() {
		for i := 0; ; i++ {
			runtime.Gosched()
			r.NodesFind(storage.RecordID(i))
		}
	}()

	time.Sleep(3 * time.Second)
}

func TestRouterNodesFind(t *testing.T) {
	cfg := Config{
		Addr:  "router",
		Nodes: []storage.ServiceAddr{"node1", "node2", "node3", "node4", "node5", "node6"},
		NodesFinder: NewNodesFinder(FakeHasher{
			t: t,
			hashes: map[storage.ServiceAddr]uint64{
				"node1": 1,
				"node2": 2,
				"node3": 3,
				"node4": 4,
				"node5": 5,
				"node6": 6,
			}}),
		ForgetTimeout: 10 * time.Millisecond,
	}

	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	l := len(cfg.Nodes)
	for _, test := range []struct {
		want  []storage.ServiceAddr
		nodes []storage.ServiceAddr
		err   error
	}{
		{err: storage.ErrNotEnoughDaemons},
		{nodes: cfg.Nodes[:l-2], err: storage.ErrNotEnoughDaemons},
		{nodes: cfg.Nodes[:l-1], want: cfg.Nodes[3:5]},
		{nodes: cfg.Nodes, want: cfg.Nodes[3:]},
		{nodes: cfg.Nodes[:l-1], want: cfg.Nodes[3:5]},
		{nodes: cfg.Nodes[:l-2], err: storage.ErrNotEnoughDaemons},
		{err: storage.ErrNotEnoughDaemons},
	} {
		t.Run(fmt.Sprintf("want=%v,nodes=%v", len(test.want), len(test.nodes)), func(t *testing.T) {
			registerNodes(t, r, test.nodes, cfg.ForgetTimeout)
			got, err := r.NodesFind(1)
			if test.err != err {
				t.Fatalf("NodesFor() expected error %v, got %v", test.err, err)
			}
			if err != nil {
				return
			}
			if !equalNodes(got, test.want) {
				t.Errorf("NodesFor() wrong nodes, got %v, want %v", got, test.want)
			}
		})
	}
}

func TestRouterNodesFind_SameHashes(t *testing.T) {
	cfg := Config{
		Addr:  "router",
		Nodes: []storage.ServiceAddr{"node1", "node2", "node3", "node4", "node5", "node6"},
		NodesFinder: NewNodesFinder(FakeHasher{
			t: t,
			hashes: map[storage.ServiceAddr]uint64{
				"node1": 1,
				"node2": 2,
				"node3": 3,
				"node4": 3,
				"node5": 5,
				"node6": 6,
			}}),
		ForgetTimeout: 10 * time.Millisecond,
	}
	r, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	for _, node := range cfg.Nodes {
		if err := r.Heartbeat(node); err != nil {
			t.Fatalf("Hearbeat() error: %v", err)
		}
	}
	for i := 0; i < 32; i++ {
		nodes, err := r.NodesFind(1)
		if err != nil {
			t.Fatalf("NodesFor() error %v", err)
		}
		if !equalNodes(nodes, cfg.Nodes[3:]) {
			t.Errorf("NodesFor() wrong nodes, got %v, want %v", nodes, cfg.Nodes[3:])
		}
	}
}
