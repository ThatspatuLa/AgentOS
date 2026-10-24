"""Publisher ordering at the executor's source-origin-independent boundary."""
from pathlib import Path
import unittest
from unittest.mock import patch

import kcos_stage2_autonomous_executor as executor


class ExecutorReconciliationTests(unittest.TestCase):
    def execute(self, fail_at=None):
        observed = []

        class Transaction:
            def __init__(self, **kwargs):
                self.arguments = kwargs

            def __enter__(self):
                observed.append("reconcile")
                return self

            def commit(self, verification):
                observed.append("commit")
                if fail_at == "commit":
                    raise RuntimeError("commit evidence unavailable")

            def __exit__(self, kind, value, traceback):
                observed.append("rollback" if kind else "exit_committed")
                return False

        def run(arguments):
            phase = {str(executor.COMPILE): "compile", str(executor.VERIFY): "verify",
                     str(executor.PUBLISH): "publish"}[arguments[0]]
            observed.append(phase)
            if phase == fail_at:
                raise RuntimeError(phase + " failed")

        with patch.object(executor.implementation_reconcile, "ImplementationReconciliation", Transaction), \
                patch.object(executor, "run", side_effect=run):
            if fail_at:
                with self.assertRaises(RuntimeError):
                    executor.finish_stage2_candidate(Path("handoff"), Path("compile"),
                                                    Path("verify"), Path("event"))
            else:
                executor.finish_stage2_candidate(Path("handoff"), Path("compile"),
                                                Path("verify"), Path("event"))
        return observed

    def test_commit_precedes_publication(self):
        self.assertEqual(self.execute(),
                         ["reconcile", "compile", "verify", "commit", "exit_committed", "publish"])

    def test_verification_failure_rolls_back_without_publication(self):
        self.assertEqual(self.execute("verify"), ["reconcile", "compile", "verify", "rollback"])

    def test_unrecordable_commit_never_publishes(self):
        self.assertEqual(self.execute("commit"),
                         ["reconcile", "compile", "verify", "commit", "rollback"])


if __name__ == "__main__":
    unittest.main()
