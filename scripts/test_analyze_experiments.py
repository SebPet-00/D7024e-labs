import json
import pathlib
import tempfile
import unittest

import analyze_experiments as analysis


class AnalysisTests(unittest.TestCase):
    def test_event_counts_and_incomplete_data(self):
        events = [
            {"event": "run_metadata", "experiment": "scale", "n": 12, "loss": 0, "seed": 1, "queries": 1},
            {"event": "lookup_start", "lookup_id": "a/1"},
            {"event": "probe", "lookup_id": "a/1", "request_id": "r", "attempt": 1},
            {"event": "probe", "lookup_id": "a/1", "request_id": "r", "attempt": 2},
            {"event": "lookup_end", "lookup_id": "a/1", "node": "a", "target": "key",
             "probes": 1, "attempts": 2, "rounds": 1, "success": True},
            {"event": "trial_result", "trial": 0, "source": "a", "target": "key", "correct": True},
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "run.jsonl"
            def write():
                path.write_text("\n".join(json.dumps(event) for event in events))
            write()
            rows = analysis.read_run(path)
            self.assertEqual(rows[0]["probes"], 1)
            self.assertEqual(rows[0]["attempts"], 2)
            events[4]["attempts"] = 1
            write()
            with self.assertRaises(ValueError):
                analysis.read_run(path)
            events[4]["attempts"] = 2
            events.pop()
            write()
            with self.assertRaises(ValueError):
                analysis.read_run(path)

    def test_variance_is_across_seed_means(self):
        rows = []
        for seed, outcomes in [(1, [0, 1]), (2, [1, 1])]:
            for value in outcomes:
                rows.append(dict(experiment="loss", n=12, loss=0.5, seed=seed,
                                 correct=value, api_success=value, probes=2, attempts=3, rounds=1))
        runs, summary = analysis.summarize(rows)
        self.assertEqual(len(runs), 2)
        self.assertEqual(summary[0]["correct_mean"], 0.75)
        self.assertEqual(summary[0]["correct_variance"], 0.125)
        self.assertEqual(summary[0]["probes_variance"], 0)


if __name__ == "__main__":
    unittest.main()
