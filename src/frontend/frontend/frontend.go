package frontend

import (
	"sync"
	"time"

	rclient "router/client"
	"router/router"
	"storage"
)

// InitTimeout is a timeout to wait after unsuccessful List() request to Router.
//
// InitTimeout -- количество времени, которое нужно подождать до следующей попытки
// отправки запроса List() в Router.
const InitTimeout = 100 * time.Millisecond

// Config stores configuration for a Frontend service.
//
// Config -- содержит конфигурацию Frontend.
type Config struct {
	// Addr is an address to listen at.
	// Addr -- слушающий адрес Frontend.
	Addr storage.ServiceAddr
	// Router is an address of Router service.
	// Router -- адрес Router service.
	Router storage.ServiceAddr

	// NC specifies client for Node.
	// NC -- клиент для node.
	NC storage.Client `yaml:"-"`
	// RC specifies client for Router.
	// RC -- клиент для router.
	RC rclient.Client `yaml:"-"`
	// NodesFinder specifies a NodeFinder to use.
	// NodesFinder -- NodesFinder, который нужно использовать в Frontend.
	NF router.NodesFinder `yaml:"-"`
}

// Frontend is a frontend service.
type Frontend struct {
	cfg Config

	nodes     []storage.ServiceAddr
	nodesOnce sync.Once
}

// New creates a new Frontend with a given cfg.
//
// New создает новый Frontend с данным cfg.
func New(cfg Config) *Frontend {
	return &Frontend{
		cfg:   cfg,
		nodes: nil,
	}
}

// Put an item to the storage if an item for the given key doesn't exist.
// Returns error otherwise.
//
// Put -- добавить запись в хранилище, если запись для данного ключа
// не существует. Иначе вернуть ошибку.
func (fe *Frontend) Put(k storage.RecordID, d []byte) error {

	nodes, err := fe.cfg.RC.NodesFind(fe.cfg.Router, k)
	if err != nil {
		return err
	}
	if len(nodes) < storage.MinRedundancy {
		return storage.ErrNotEnoughDaemons
	}

	wg := sync.WaitGroup{}
	results := make(chan error, len(nodes))
	wg.Add(len(nodes))
	for i, node := range nodes {
		go func(nodeIdx int, node storage.ServiceAddr) {
			defer wg.Done()
			results <- fe.cfg.NC.Put(node, k, d)
		}(i, node)
	}
	wg.Wait()
	close(results)

	return checkErrors(results)
}

// Del an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Del -- удалить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Del(k storage.RecordID) error {

	nodes, err := fe.cfg.RC.NodesFind(fe.cfg.Router, k)
	if err != nil {
		return err
	}
	if len(nodes) < storage.MinRedundancy {
		return storage.ErrNotEnoughDaemons
	}

	wg := sync.WaitGroup{}

	results := make(chan error, len(nodes))
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func(node storage.ServiceAddr) {
			defer wg.Done()
			results <- fe.cfg.NC.Del(node, k)
		}(node)
	}
	wg.Wait()
	close(results)

	return checkErrors(results)
}

func checkErrors(errs <-chan error) error {
	oks := 0
	resMap := make(map[error]int)
	for err := range errs {
		if err == nil {
			oks++
		}
		if _, ok := resMap[err]; !ok {
			resMap[err] = 1
		} else {
			resMap[err]++
		}
	}

	if oks > storage.MinRedundancy {
		return nil
	}
	for err, n := range resMap {
		if n >= storage.MinRedundancy {
			return err
		}
	}
	return storage.ErrQuorumNotReached
}

// Get an item from the storage if an item exists for the given key.
// Returns error otherwise.
//
// Get -- получить запись из хранилища, если запись для данного ключа
// существует. Иначе вернуть ошибку.
func (fe *Frontend) Get(k storage.RecordID) ([]byte, error) {
	fe.nodesOnce.Do(fe.initNodes)

	nodes := fe.cfg.NF.NodesFind(k, fe.nodes)
	if len(nodes) < storage.MinRedundancy {
		return nil, storage.ErrNotEnoughDaemons
	}

	resChan := make(chan getResult, len(nodes))
	endChan := make(chan getResult)

	go checkResults(resChan, endChan)

	go func() {
		var wg sync.WaitGroup
		wg.Add(len(nodes))
		for _, node := range nodes {
			go func(node storage.ServiceAddr) {
				defer wg.Done()
				d, err := fe.cfg.NC.Get(node, k)
				resChan <- struct {
					d   []byte
					err error
				}{d, err}
			}(node)
		}
		wg.Wait()
		close(resChan)
	}()

	res := <-endChan

	return res.d, res.err
}

func (fe *Frontend) initNodes() {
	var err error
	var nodes []storage.ServiceAddr

	nodes, err = fe.cfg.RC.List(fe.cfg.Router)
	if err != nil {
		time.Sleep(InitTimeout)
		nodes, err = fe.cfg.RC.List(fe.cfg.Router)
		if err != nil {
			return
		}
	}
	fe.nodes = nodes
	return
}

type getResult struct {
	d   []byte
	err error
}

func checkResults(results <-chan getResult, endChan chan<- getResult) {
	resMap := make(map[string]int)
	errMap := make(map[string]int)
	found := false

	for res := range results {
		if found {
			continue
		}

		if res.err == nil {
			key := string(res.d)
			updateKey(resMap, key)

			if resMap[key] >= storage.MinRedundancy {
				endChan <- res
				found = true
			}
		} else {
			key := res.err.Error()
			updateKey(errMap, key)

			if errMap[key] >= storage.MinRedundancy {
				endChan <- res
				found = true
			}
		}
	}

	if !found {
		endChan <- struct {
			d   []byte
			err error
		}{
			err: storage.ErrQuorumNotReached,
		}
	}
}

func updateKey(m map[string]int, k string) {
	if _, ok := m[k]; !ok {
		m[k] = 1
	} else {
		m[k]++
	}
}
