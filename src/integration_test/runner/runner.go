package runner

import (
	"sync"
	"time"

	"frontend/frontend"
	"node/node"
	"router/client"
	"router/router"
	"router/server"
	"storage"
)

const heartbeat = time.Second

type nodeService struct {
	node *node.Node
	srv  *storage.Server
}

type routerService struct {
	r   *router.Router
	srv *server.Server
}

type frontendService struct {
	fe  *frontend.Frontend
	srv *storage.Server
}

type Runner struct {
	sync.Mutex

	router routerService
	nodes  map[storage.ServiceAddr]nodeService
	fe     []frontendService
}

func (r *Runner) StartNodes(nodes []storage.ServiceAddr, router storage.ServiceAddr) {
	r.Lock()
	defer r.Unlock()
	if r.nodes != nil {
		panic("already running")
	}
	r.nodes = make(map[storage.ServiceAddr]nodeService)
	for _, addr := range nodes {
		cfg := node.Config{
			Addr:      addr,
			Router:    router,
			Heartbeat: heartbeat,
			Client:    client.New(),
		}
		n := node.New(cfg)
		n.Heartbeats()
		srv := storage.NewServer(n, string(addr))
		r.nodes[addr] = nodeService{
			node: n,
			srv:  srv,
		}
		go func(srv *storage.Server) {
			if err := srv.ListenAndServe(); err != nil {
				panic("error serving")
			}
		}(srv)
	}
}

func (r *Runner) StopNodes() {
	r.Lock()
	defer r.Unlock()
	for _, n := range r.nodes {
		n.node.Stop()
		n.srv.Stop()
	}
	r.nodes = nil
}

func (r *Runner) StartRouter(addr storage.ServiceAddr, nodes []storage.ServiceAddr) {
	r.Lock()
	defer r.Unlock()
	cfg := router.Config{
		Addr:          addr,
		Nodes:         nodes,
		ForgetTimeout: 5 * heartbeat,
		NodesFinder:   router.NewNodesFinder(router.NewMD5Hasher()),
	}

	rtr, err := router.New(cfg)
	if err != nil {
		panic("error creating router")
	}
	srv := server.New(rtr, string(addr))

	r.router = routerService{
		r:   rtr,
		srv: srv,
	}

	go func(srv *server.Server) {
		if err := srv.ListenAndServe(); err != nil {
			panic("error serving")
		}
	}(srv)
}

func (r *Runner) StopRouter() {
	r.Lock()
	defer r.Unlock()
	r.router.srv.Stop()
}

func (r *Runner) StartFrontends(addrs []storage.ServiceAddr, routerAddr storage.ServiceAddr) {
	r.Lock()
	defer r.Unlock()
	r.stopFrontends()
	for _, addr := range addrs {
		cfg := frontend.Config{
			Addr:   addr,
			Router: routerAddr,
			NC:     storage.NewClient(),
			RC:     client.New(),
			NF:     router.NewNodesFinder(router.NewMD5Hasher()),
		}

		fe := frontend.New(cfg)
		srv := storage.NewServer(fe, string(addr))
		r.fe = append(r.fe, frontendService{
			fe:  fe,
			srv: srv,
		})

		go func(srv *storage.Server) {
			if err := srv.ListenAndServe(); err != nil {
				panic("error serving")
			}
		}(srv)
	}
}

func (r *Runner) StopFrontends() {
	r.Lock()
	defer r.Unlock()
	r.stopFrontends()
}

func (r *Runner) stopFrontends() {
	for _, fe := range r.fe {
		fe.srv.Stop()
	}
	r.fe = nil
}

func (r *Runner) Start(router storage.ServiceAddr, fe, nodes, aliveNodes []storage.ServiceAddr) {
	r.StartRouter(router, nodes)
	r.StartNodes(aliveNodes, router)
	r.StartFrontends(fe, router)
	time.Sleep(3 * heartbeat / 2)
}

func (r *Runner) Stop() {
	r.StopRouter()
	r.StopNodes()
	r.StopFrontends()
}
