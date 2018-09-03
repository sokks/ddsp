package main

import (
	"fmt"
	"log"
	"os"

	yaml "gopkg.in/yaml.v2"

	"node/node"
	"router/client"
	"storage"
)

func usage() {
	fmt.Println("node -- service to store data for the distributed KV storage")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Printf("%24s\n\n", "node <conf.yaml>")
}

func parseConfig(fname string) (cfg node.Config, err error) {
	f, err := os.Open(fname)
	if err != nil {
		return cfg, fmt.Errorf("Failed to open config file %q: %v", fname, err)
	}
	defer f.Close()
	dec := yaml.NewDecoder(f)
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("Failed to parse config file %q: %v", fname, err)

	}
	if cfg.Addr == "" {
		return cfg, fmt.Errorf("Failed to parse config file %q: Addr should be set", fname)
	}
	if cfg.Router == "" {
		return cfg, fmt.Errorf("Failed to parse config file %q: Router should be set", fname)
	}
	if cfg.Heartbeat == 0 {
		return cfg, fmt.Errorf("Failed to parse config file %q: Hearbeat should be set", fname)
	}

	return cfg, nil
}

func main() {
	if len(os.Args) != 2 {
		usage()
		os.Exit(1)
	}

	cfg, err := parseConfig(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	cfg.Client = client.New()

	st := node.New(cfg)
	st.Heartbeats()

	srv := storage.NewServer(st, string(cfg.Addr))
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
