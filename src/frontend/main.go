package main

import (
	"fmt"
	"log"
	"os"

	yaml "gopkg.in/yaml.v2"

	"frontend/frontend"
	rclient "router/client"
	"router/router"
	"storage"
)

func usage() {
	fmt.Println("frontend -- service to access the distributed KV storage")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Printf("%24s\n\n", "frontend <conf.yaml>")
}

func parseConfig(fname string) (cfg frontend.Config, err error) {
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

	cfg.NC = storage.NewClient()
	cfg.RC = rclient.New()

	hasher := router.NewMD5Hasher()
	cfg.NF = router.NewNodesFinder(hasher)

	fe := frontend.New(cfg)
	srv := storage.NewServer(fe, string(cfg.Addr))
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
