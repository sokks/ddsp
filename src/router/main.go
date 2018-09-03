package main

import (
	"fmt"
	"log"
	"os"

	yaml "gopkg.in/yaml.v2"

	"router/router"
	"router/server"
)

func usage() {
	fmt.Println("router -- service to store cluser scheme of the distributed KV storage")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Printf("%24s\n\n", "router  <conf.yaml>")
}

func parseConfig(fname string) (cfg router.Config, err error) {
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
	if cfg.Nodes == nil {
		return cfg, fmt.Errorf("Failed to parse config file %q: Nodes should be set", fname)
	}
	if cfg.ForgetTimeout == 0 {
		return cfg, fmt.Errorf("Failed to parse config file %q: ForgetTimeout should be set and be positive", fname)
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

	hasher := router.NewMD5Hasher()
	cfg.NodesFinder = router.NewNodesFinder(hasher)

	r, err := router.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create router: %v", err)
	}

	srv := server.New(r, string(cfg.Addr))

	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
