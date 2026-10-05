import subprocess
import unittest
from unittest.mock import patch

import lab


def state(running, logs):
    return ({"Name": "/node", "State": {
        "Running": running, "Status": "running" if running else "exited",
        "ExitCode": 0 if running else 1}}, logs)


class StartupTests(unittest.TestCase):
    @patch("lab.time.sleep")
    @patch("lab.subprocess.run")
    @patch("lab.snapshot")
    def test_transient_join_recovers(self, snapshot, run, sleep):
        snapshot.side_effect = [state(False, "join: RPC timed out"),
                                state(True, "Ready 172.19.0.3:8000\n")]
        lab.wait_node("peer")
        self.assertEqual(run.call_args.args[0], ["docker", "start", "peer"])
        self.assertEqual(run.call_count, 1)

    @patch("lab.time.sleep")
    @patch("lab.subprocess.run")
    @patch("lab.snapshot", return_value=state(False, "join: RPC timed out"))
    def test_retry_budget(self, snapshot, run, sleep):
        with self.assertRaisesRegex(RuntimeError, "stopped"):
            lab.wait_node("peer", retries=2)
        self.assertEqual(run.call_count, 2)

    @patch("lab.subprocess.run")
    @patch("lab.snapshot", return_value=state(False, "invalid listen port"))
    def test_fatal_error_is_not_retried(self, snapshot, run):
        with self.assertRaisesRegex(RuntimeError, "invalid listen port"):
            lab.wait_node("peer")
        run.assert_not_called()

    @patch("lab.subprocess.run")
    @patch("lab.subprocess.check_output")
    def test_logs_are_scoped_to_current_start(self, inspect, logs):
        inspect.return_value = '[{"State":{"StartedAt":"2026-10-05T12:00:00Z"}}]'
        logs.return_value = subprocess.CompletedProcess([], 0, "", "join: RPC timed out")
        self.assertEqual(lab.snapshot("peer")[1], "join: RPC timed out")
        self.assertEqual(logs.call_args.args[0],
                         ["docker", "logs", "--since", "2026-10-05T12:00:00Z", "peer"])

    @patch("lab.wait_ready")
    @patch("lab.wait_node")
    @patch("lab.subprocess.check_output", return_value="172.19.0.2\n")
    @patch("lab.compose")
    def test_resume_grows_without_scaling_down(self, compose, inspect, wait, ready):
        def response(project, *args, **kwargs):
            output = ""
            if args == ("ps", "-a", "-q", "node"):
                output = "n1\nn2\nn3\nn4\n"
            elif args == ("ps", "-q", "bootstrap"):
                output = "bootstrap\n"
            return subprocess.CompletedProcess([], 0, output, "")
        compose.side_effect = response
        lab.start("test", 10, batch_size=3)
        self.assertEqual([call.args[1] for call in ready.call_args_list], [5, 8, 10])


if __name__ == "__main__":
    unittest.main()