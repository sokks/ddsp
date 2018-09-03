package server

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	"router/pb"
	"router/router"
	"storage"
)

type Server struct {
	addr string
	rtr  *router.Router
	srv  *grpc.Server
}

func New(rtr *router.Router, addr string) *Server {
	return &Server{
		addr: addr,
		rtr:  rtr,
		srv:  grpc.NewServer(),
	}
}

func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("Failed to listen: %v", err)
	}

	pb.RegisterRouterServer(s.srv, s)
	log.Printf("Starting router service at %v", s.addr)
	return s.srv.Serve(l)
}

func (s *Server) Stop() {
	s.srv.Stop()
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HBRequest) (*pb.HBReply, error) {
	node := storage.ServiceAddr(req.Node)
	log.Printf("Hearbeat request: node = %q", node)

	err := s.rtr.Heartbeat(node)
	status := storage.ErrToStatus(err)

	reply := pb.HBReply{
		Status: int32(status),
	}
	if status == storage.StatusUnknown {
		reply.Error = err.Error()
	}
	return &reply, nil
}

func (s *Server) NodesFind(ctx context.Context, req *pb.NFRequest) (*pb.NFReply, error) {
	key := storage.RecordID(req.Key)
	log.Printf("NodesFind request: key = %v", key)

	nodes, err := s.rtr.NodesFind(key)
	status := storage.ErrToStatus(err)

	reply := pb.NFReply{
		Status: int32(status),
	}
	if status == storage.StatusUnknown {
		reply.Error = err.Error()
		return &reply, nil
	}

	reply.Nodes = make([]string, 0, len(nodes))
	for _, node := range nodes {
		reply.Nodes = append(reply.Nodes, string(node))
	}
	return &reply, nil
}

func (s *Server) List(ctx context.Context, req *pb.Empty) (*pb.ListReply, error) {
	log.Printf("List request")

	nodes := s.rtr.List()
	reply := pb.ListReply{
		Status: int32(storage.StatusOk),
	}
	reply.Nodes = make([]string, 0, len(nodes))
	for _, node := range nodes {
		reply.Nodes = append(reply.Nodes, string(node))
	}
	return &reply, nil
}
