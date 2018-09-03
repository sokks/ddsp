package client

import (
	"context"
	"errors"
	"fmt"
	"log"

	"google.golang.org/grpc"

	"router/pb"
	"storage"
)

type Client interface {
	Heartbeat(router, node storage.ServiceAddr) error
	NodesFind(router storage.ServiceAddr, k storage.RecordID) ([]storage.ServiceAddr, error)
	List(router storage.ServiceAddr) ([]storage.ServiceAddr, error)
}

type RouterClient struct{}

var defaultClient Client = RouterClient{}

func New() Client {
	return defaultClient
}

func (c RouterClient) do(addr storage.ServiceAddr, cb func(client pb.RouterClient) ([]storage.ServiceAddr, error)) ([]storage.ServiceAddr, error) {
	ctx, cancel := context.WithTimeout(context.Background(), storage.Timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, string(addr), grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Error dialing %q: %v", addr, err)
	}
	defer conn.Close()
	client := pb.NewRouterClient(conn)
	return cb(client)
}

func (c RouterClient) Heartbeat(router, node storage.ServiceAddr) error {
	log.Printf("Hearbeat request to %q", router)
	_, err := c.do(router, func(client pb.RouterClient) ([]storage.ServiceAddr, error) {
		ctx, cancel := context.WithTimeout(context.Background(), storage.Timeout)
		defer cancel()
		req := pb.HBRequest{
			Node: string(node),
		}
		reply, err := client.Heartbeat(ctx, &req)
		if err != nil {
			return nil, err
		}

		status := storage.StatusCode(reply.Status)

		if status == storage.StatusOk {
			return nil, nil
		}

		if err := status.ToError(); err != storage.ErrUnknownStatus {
			return nil, err
		}
		return nil, errors.New(reply.Error)
	})
	return err
}

func (c RouterClient) NodesFind(router storage.ServiceAddr, k storage.RecordID) ([]storage.ServiceAddr, error) {
	log.Printf("NodesFind request: key = %v", k)
	return c.do(router, func(client pb.RouterClient) ([]storage.ServiceAddr, error) {
		ctx, cancel := context.WithTimeout(context.Background(), storage.Timeout)
		defer cancel()
		req := pb.NFRequest{
			Key: uint32(k),
		}
		reply, err := client.NodesFind(ctx, &req)
		if err != nil {
			return nil, err
		}

		status := storage.StatusCode(reply.Status)

		if status == storage.StatusOk {
			nodes := make([]storage.ServiceAddr, 0, len(reply.Nodes))
			for _, node := range reply.Nodes {
				nodes = append(nodes, storage.ServiceAddr(node))
			}
			return nodes, nil
		}

		if err := status.ToError(); err != storage.ErrUnknownStatus {
			return nil, err
		}
		return nil, errors.New(reply.Error)
	})
}

func (c RouterClient) List(router storage.ServiceAddr) ([]storage.ServiceAddr, error) {
	log.Printf("List request")
	return c.do(router, func(client pb.RouterClient) ([]storage.ServiceAddr, error) {
		ctx, cancel := context.WithTimeout(context.Background(), storage.Timeout)
		defer cancel()
		reply, err := client.List(ctx, &pb.Empty{})
		if err != nil {
			return nil, err
		}

		status := storage.StatusCode(reply.Status)

		if status == storage.StatusOk {
			nodes := make([]storage.ServiceAddr, 0, len(reply.Nodes))
			for _, node := range reply.Nodes {
				nodes = append(nodes, storage.ServiceAddr(node))
			}
			return nodes, nil
		}

		if err := status.ToError(); err != storage.ErrUnknownStatus {
			return nil, err
		}
		return nil, errors.New(reply.Error)
	})
}
