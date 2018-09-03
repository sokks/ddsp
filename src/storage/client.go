package storage

import (
	"context"
	"errors"
	"fmt"
	"log"

	"google.golang.org/grpc"

	"storage/pb"
)

type Client interface {
	Put(node ServiceAddr, k RecordID, d []byte) error
	Get(node ServiceAddr, k RecordID) ([]byte, error)
	Del(node ServiceAddr, k RecordID) error
}

type StorageClient struct{}

var defaultClient Client = StorageClient{}

func NewClient() Client {
	return defaultClient
}

func (c StorageClient) do(addr ServiceAddr, cb func(client pb.StorageClient) ([]byte, error)) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, string(addr), grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Error dialing %q: %v", addr, err)
	}
	defer conn.Close()
	client := pb.NewStorageClient(conn)
	return cb(client)
}

func (c StorageClient) Put(node ServiceAddr, k RecordID, d []byte) error {
	log.Printf("Putting record to %q, key = %v", node, k)
	_, err := c.do(node, func(client pb.StorageClient) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), Timeout)
		defer cancel()
		req := pb.PutRequest{
			Key:  uint32(k),
			Data: d,
		}
		reply, err := client.Put(ctx, &req)
		if err != nil {
			return nil, err
		}
		status := StatusCode(reply.Status)
		if status == StatusOk {
			return nil, nil
		}
		if err := status.ToError(); err != ErrUnknownStatus {
			return nil, err
		}
		return nil, errors.New(reply.Error)
	})
	return err
}

func (c StorageClient) Get(node ServiceAddr, k RecordID) ([]byte, error) {
	log.Printf("Getting record from %q, key = %v", node, k)
	return c.do(node, func(client pb.StorageClient) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), Timeout)
		defer cancel()
		req := pb.GetRequest{
			Key: uint32(k),
		}
		reply, err := client.Get(ctx, &req)
		if err != nil {
			return nil, err
		}
		status := StatusCode(reply.Status)
		if status == StatusOk {
			return reply.Data, nil
		}
		if err := status.ToError(); err != ErrUnknownStatus {
			return nil, err
		}
		return nil, errors.New(reply.Error)
	})
}

func (c StorageClient) Del(node ServiceAddr, k RecordID) error {
	log.Printf("Deleting record from %q, key = %v", node, k)
	_, err := c.do(node, func(client pb.StorageClient) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), Timeout)
		defer cancel()
		req := pb.DelRequest{
			Key: uint32(k),
		}
		reply, err := client.Del(ctx, &req)
		if err != nil {
			return nil, err
		}
		status := StatusCode(reply.Status)
		if status == StatusOk {
			return nil, nil
		}
		if err := status.ToError(); err != ErrUnknownStatus {
			return nil, err
		}
		return nil, errors.New(reply.Error)
	})
	return err
}
