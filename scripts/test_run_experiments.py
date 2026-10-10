import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import run_experiments as runner


def command_output(command, **kwargs):
    if command[0] == "go":
        return json.dumps({"Sizes": [25, 2000], "Losses": [0, 0.52, 0.7], "Seeds": [11, 22]})
    return "revision"


class PipelineTests(unittest.TestCase):
    def test_dry_run_creates_nothing_and_needs_no_tools(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "new"
            with patch.object(runner.subprocess, "run") as run, contextlib.redirect_stdout(io.StringIO()):
                runner.run_pipeline(output, dry_run=True)
            run.assert_not_called()
            self.assertFalse(output.exists())

    def test_existing_output_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "keep.txt"
            marker.write_text("original")
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(ValueError):
                runner.run_pipeline(Path(directory))
            self.assertEqual(marker.read_text(), "original")

    def test_failed_workload_skips_analysis_and_records_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "new"
            with patch.object(runner.shutil, "which", return_value="go"), \
                    patch.object(runner.subprocess, "check_output", side_effect=command_output), \
                    patch.object(runner.subprocess, "run", side_effect=[
                        None, subprocess.CalledProcessError(1, ["go"]),
                    ]) as run, contextlib.redirect_stdout(io.StringIO()), \
                    self.assertRaises(subprocess.CalledProcessError):
                runner.run_pipeline(output)
            self.assertEqual(run.call_count, 2)  # dependency check, workload only
            self.assertEqual(json.loads((output / "manifest.json").read_text())["status"], "incomplete")

    def test_success_runs_analysis_with_same_python_and_marks_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "new"
            with patch.object(runner.shutil, "which", return_value="go"), \
                    patch.object(runner.subprocess, "check_output", side_effect=command_output), \
                    patch.object(runner.subprocess, "run") as run, \
                    contextlib.redirect_stdout(io.StringIO()):
                runner.run_pipeline(output, queries=2)
            self.assertEqual(run.call_count, 3)
            self.assertEqual(run.call_args_list[1].args[0][-2:], ["-queries", "2"])
            self.assertEqual(run.call_args.args[0][0], runner.sys.executable)
            self.assertEqual(json.loads((output / "manifest.json").read_text())["status"], "complete")
            self.assertEqual(json.loads((output / "manifest.json").read_text())["settings"]["Sizes"], [25, 2000])


if __name__ == "__main__":
    unittest.main()
