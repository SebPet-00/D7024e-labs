package main

import (
	"d7024e/experiments"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

func main() {
	settings := experiments.DefaultSettings()
	flag.StringVar(&settings.Output, "out", settings.Output, "new directory for raw JSONL runs")
	flag.IntVar(&settings.Queries, "queries", settings.Queries, "lookups per seed and condition")
	flag.DurationVar(&settings.RPCTimeout, "rpc-timeout", settings.RPCTimeout, "timeout per RPC attempt")
	flag.DurationVar(&settings.QueryTimeout, "query-timeout", settings.QueryTimeout, "overall lookup deadline")
	describe := flag.Bool("describe", false, "print settings as JSON without running experiments")
	flag.Parse()
	if *describe {
		if err := json.NewEncoder(os.Stdout).Encode(settings); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := experiments.Run(settings); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
