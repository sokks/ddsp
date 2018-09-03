package main

import (
	"flag"
	"fmt"
	"math"
	"os"

	"storage"
)

const (
	get = "get"
	put = "put"
	del = "del"
)

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  clikv [-h]")
	fmt.Println("  clikv <command> -s=<addr> -k=<key> [-v=<val>]")

	fmt.Println()
	fmt.Println("List of available commands:")
	fmt.Printf("  %s\n", get)
	fmt.Printf("  %s\n", put)
	fmt.Printf("  %s\n", del)

	fmt.Println()
	fmt.Println("List of available options:")
	flag.PrintDefaults()
}

var (
	addr = flag.String("s", "", "address to send request to (e.g. localhost:7319) (REQUIRED)")
	key  = flag.Int64("k", -1, "key (REQUIRED)")
	val  = flag.String("v", "", "value")
	help = flag.Bool("h", false, "show this help message")
)

func main() {
	flag.Parse()
	if *help {
		usage()
		os.Exit(0)
	}
	if *addr == "" {
		fmt.Fprintln(os.Stderr, "-s cannot be empty")
		os.Exit(2)
	}
	if *key < 0 || *key > math.MaxUint32 {
		fmt.Fprintln(os.Stderr, "-k should be set to a uint32 value")
		os.Exit(2)
	}
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "exactly one command should be provided")
		os.Exit(2)

	}

	client := storage.NewClient()
	node := storage.ServiceAddr(*addr)

	k := storage.RecordID(*key)
	data := []byte(*val)

	switch flag.Arg(0) {
	case put:
		if err := client.Put(node, k, data); err != nil {
			fmt.Fprintf(os.Stderr, "Error putting record: %v\n", err)
			os.Exit(1)
		}
	case get:
		b, err := client.Get(node, k)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting record: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Got record %q\n", b)
	case del:
		if err := client.Del(node, k); err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting record: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown command %q", flag.Arg(0))
		os.Exit(2)
	}
}
