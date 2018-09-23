package node

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"storage"
)

var cfg = Config{
	Heartbeat: time.Second,
}

func TestPutGet(t *testing.T) {
	s := New(cfg)
	key := storage.RecordID(1)
	data := []byte("some data")

	if _, err := s.Get(key); err != storage.ErrRecordNotFound {
		t.Fatalf("Get(): got error %v, want %v", err, storage.ErrRecordNotFound)
	}

	if err := s.Put(key, data); err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	got, err := s.Get(key)
	if err != nil {
		t.Errorf("Get() error: %v", err)
	}
	if !reflect.DeepEqual(got, data) {
		t.Errorf("Wrong data: got %s, want %s", got, data)
	}

	if err := s.Put(key, data); err != storage.ErrRecordExists {
		t.Fatalf("Put() got error: %v, want %v", err, storage.ErrRecordExists)
	}

	got, err = s.Get(key)
	if err != nil {
		t.Errorf("Get() error: %v", err)
	}
	if !reflect.DeepEqual(got, data) {
		t.Errorf("Wrong data: got %s, want %s", got, data)
	}
}

func TestDel(t *testing.T) {
	s := New(cfg)
	key := storage.RecordID(1)
	data := []byte("some data")
	if err := s.Del(key); err != storage.ErrRecordNotFound {
		t.Fatalf("Del(): got error %v, want %v", err, storage.ErrRecordNotFound)
	}

	if err := s.Put(key, data); err != nil {
		t.Fatalf("Put() error: %v", err)
	}

	if err := s.Del(key); err != nil {
		t.Errorf("Del() error %v", err)
	}

	if _, err := s.Get(key); err != storage.ErrRecordNotFound {
		t.Errorf("Get(): got error %v, want %v", err, storage.ErrRecordNotFound)
	}
}

func TestParallelOps(t *testing.T) {
	s := New(cfg)
	var keys []storage.RecordID
	var d [][]byte

	const n = 100
	for i := 0; i < n; i++ {
		keys = append(keys, storage.RecordID(i))
		d = append(d, []byte(fmt.Sprintf("data%d", i)))
	}

	go func() {
		for {
			runtime.Gosched()
			k := rand.Intn(n)
			got, err := s.Get(keys[k])
			if err != nil {
				if err != storage.ErrRecordNotFound {
					t.Fatalf("Get(): got error %v, want %v", err, storage.ErrRecordNotFound)
				}
				continue
			}
			if !reflect.DeepEqual(got, d[k]) {
				t.Fatalf("Wrong data: got %s, want %s", got, d)
			}
		}
	}()

	go func() {
		for {
			runtime.Gosched()
			k := rand.Intn(n)
			s.Put(keys[k], d[k])
		}
	}()

	go func() {
		for {
			runtime.Gosched()
			k := rand.Intn(n)
			err := s.Del(keys[k])
			if err == nil {
				continue
			}
			if err != storage.ErrRecordNotFound {
				t.Fatalf("Del(%v): error %v, want %v", keys[k], err, storage.ErrRecordNotFound)
			}
		}
	}()
	time.Sleep(3 * time.Second)
}

type FakeClient struct {
	sync.Mutex
	t *testing.T

	d    time.Duration
	skip time.Duration

	last time.Time
	n    int
}

func (c *FakeClient) NodesFind(router storage.ServiceAddr, k storage.RecordID) ([]storage.ServiceAddr, error) {
	return nil, nil
}
func (c *FakeClient) List(router storage.ServiceAddr) ([]storage.ServiceAddr, error) { return nil, nil }

func (c *FakeClient) Heartbeat(router, node storage.ServiceAddr) error {
	c.Lock()
	defer c.Unlock()
	if c.n == 2 {
		// long heartbeat
		time.Sleep(c.skip)
	} else {
		if t := time.Since(c.last); t > 2*c.d {
			c.t.Fatalf("Interval between heartbeats was too long: %v", t)
		}
	}
	c.last = time.Now()
	c.n++
	return nil
}

func TestHeartbeat(t *testing.T) {
	var (
		d    = 100 * time.Millisecond
		skip = 300 * time.Millisecond
	)

	c := &FakeClient{
		t:    t,
		d:    d,
		skip: skip,
		last: time.Now(),
	}

	s := New(Config{
		Client:    c,
		Addr:      "test",
		Heartbeat: d,
	})

	s.Heartbeats()
	time.Sleep(time.Second)

	c.Lock()
	defer c.Unlock()
	if t := time.Since(c.last); t > 2*c.d {
		c.t.Fatalf("Interval between heartbeats was too long: %v", t)
	}
}

type FakeClientStopHeartbeat struct {
	sync.Mutex
	stopped  bool
	received bool
	t        *testing.T
}

func (c *FakeClientStopHeartbeat) NodesFind(router storage.ServiceAddr, k storage.RecordID) ([]storage.ServiceAddr, error) {
	return nil, nil
}
func (c *FakeClientStopHeartbeat) List(router storage.ServiceAddr) ([]storage.ServiceAddr, error) {
	return nil, nil
}

func (c *FakeClientStopHeartbeat) Heartbeat(router, node storage.ServiceAddr) error {
	c.Lock()
	defer c.Unlock()
	if c.stopped {
		c.t.Fatalf("Heartbeat() request after heartbeats were stopped")
	}
	c.received = true
	return nil
}

func TestStopHeartbeat(t *testing.T) {
	c := &FakeClientStopHeartbeat{t: t}
	s := New(Config{
		Client:    c,
		Addr:      "test",
		Heartbeat: 100 * time.Millisecond,
	})

	s.Heartbeats()

	time.Sleep(500 * time.Millisecond)
	s.Stop()
	c.Lock()
	c.stopped = true
	c.Unlock()
	time.Sleep(500 * time.Millisecond)
	c.Lock()
	if !c.received {
		t.Errorf("No heartbeat was received")
	}
	c.Unlock()
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())
	os.Exit(m.Run())
}
