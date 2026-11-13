"""Fast regression checks for the manual HTTP runner's admission guards."""
import contextlib
import importlib.util
import io
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch


class RunnerGuards(unittest.TestCase):
    """Exercise CLI validation and actual-affinity checks without starting a server."""

    @classmethod
    def setUpClass(cls):
        """Import the runner with all generated files confined to a temporary directory."""
        cls.directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.directory.cleanup)
        path = Path(__file__).with_name('dynamic_search_batching.py')
        spec = importlib.util.spec_from_file_location('dynamic_search_batching_runner', path)
        cls.runner = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {'QDRANT_BATCH_RESULTS': cls.directory.name}):
            spec.loader.exec_module(cls.runner)

    def test_check_rejects_disabled_and_control_modes(self):
        """Neither an omitted max nor explicit 0/1 may start a coalescing check."""
        for args in [[], ['--max', '0'], ['--max', '1']]:
            with (
                self.subTest(args=args),
                patch.object(sys, 'argv', ['runner', 'check', '--binary', '/unused', *args]),
                patch.object(self.runner, 'start') as start,
                contextlib.redirect_stderr(io.StringIO()) as error,
            ):
                with self.assertRaises(SystemExit) as caught:
                    self.runner.main()
                self.assertEqual(caught.exception.code, 2)
                self.assertIn('check requires --max >= 2', error.getvalue())
                start.assert_not_called()

    def test_check_accepts_merging_modes(self):
        """The minimum merging cap and the documented cap both reach server startup."""
        for maximum in [2, 64]:
            with (
                self.subTest(maximum=maximum),
                patch.object(sys, 'argv', ['runner', 'check', '--binary', '/unused', '--max', str(maximum)]),
                patch.object(self.runner, 'start', side_effect=RuntimeError('startup reached')) as start,
            ):
                with self.assertRaisesRegex(RuntimeError, 'startup reached'):
                    self.runner.main()
                self.assertEqual(start.call_args.args[1], maximum)

    def test_affinities_use_actual_cpu_sets(self):
        """Disjoint masks pass while a broad or identical client mask is rejected."""
        for client, overlaps in [({13, 14, 15}, False), (set(range(16)), True), ({1, 2, 3}, True)]:
            with (
                self.subTest(client=client),
                patch.object(self.runner.os, 'sched_getaffinity',
                             side_effect=lambda pid: {1, 2, 3} if pid == 123 else client),
            ):
                if overlaps:
                    with self.assertRaisesRegex(ValueError, 'CPU masks overlap'):
                        self.runner.validate_cpu_affinity(123)
                else:
                    self.runner.validate_cpu_affinity(123)

    def test_overlap_stops_server_before_correctness_load(self):
        """A failed affinity guard still cleans up the owned server and generates no queries."""
        server = (SimpleNamespace(pid=123), None)
        with (
            patch.object(sys, 'argv', ['runner', 'check', '--binary', '/unused', '--max', '64']),
            patch.object(self.runner, 'start', return_value=server),
            patch.object(self.runner, 'stop') as stop,
            patch.object(self.runner, 'queries') as queries,
            patch.object(self.runner.os, 'sched_getaffinity', return_value={1, 2, 3}),
        ):
            with self.assertRaisesRegex(ValueError, 'CPU masks overlap'):
                self.runner.main()
            stop.assert_called_once_with(server)
            queries.assert_not_called()


if __name__ == '__main__':
    unittest.main()
