package experiments

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExperimentWorkloadAndOracle(t *testing.T) {
	settings := DefaultSettings()
	settings.Output = t.TempDir()
	settings.Sizes = []int{12}
	settings.LossNodes = 12
	settings.Losses = []float64{0, 1}
	settings.Seeds = []int64{17}
	settings.Queries = 2
	settings.RPCTimeout = 20 * time.Millisecond
	settings.QueryTimeout = 300 * time.Millisecond
	if err := Run(settings); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(settings.Output, "*.jsonl"))
	if err != nil || len(paths) != 3 {
		t.Fatal("missing runs")
	}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(file)
		results, ends := 0, 0
		loss := 0.0
		for scanner.Scan() {
			var event map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				t.Fatal(err)
			}
			switch event["event"] {
			case "run_metadata":
				loss = event["loss"].(float64)
			case "lookup_end":
				ends++
			case "trial_result":
				results++
				if event["correct"].(bool) != (loss == 0) {
					t.Fatalf("incorrect result at loss %v", loss)
				}
			}
		}
		file.Close()
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
		if results != 2 || ends != 2 {
			t.Fatal("missing records")
		}
	}
	if err := Run(settings); err == nil {
		t.Fatal("overwrote existing data")
	}
}
