"""
Unit tests for databricks/delta_utils.py.

PySpark is NOT imported — all Spark interactions are mocked so the tests run in
any Python environment without a Spark installation.
"""

from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, call, patch

# ---------------------------------------------------------------------------
# Module under test
# ---------------------------------------------------------------------------
from databricks.delta_utils import delta_conflict_retry, delta_write_with_retry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conflict_exc(marker: str = "ConcurrentWriteException") -> Exception:
    """Return an Exception whose str() contains a recognised conflict marker."""
    return Exception(f"org.apache.spark.sql.delta.{marker}: table locked")


def _make_non_conflict_exc() -> Exception:
    return PermissionError("s3: Access Denied on dbfs:/mnt/gold/events")


# ---------------------------------------------------------------------------
# Tests for delta_write_with_retry
# ---------------------------------------------------------------------------

class TestDeltaWriteWithRetry(unittest.TestCase):

    # ------------------------------------------------------------------
    # Test 1 — succeeds on the third attempt after two conflict errors
    # ------------------------------------------------------------------
    @patch("databricks.delta_utils.time.sleep", return_value=None)
    @patch("databricks.delta_utils.logger")
    def test_retry_succeeds_after_conflict(self, mock_logger: MagicMock, mock_sleep: MagicMock) -> None:
        """
        When the underlying write raises a conflict error on attempts 1 and 2
        and succeeds on attempt 3, delta_write_with_retry should return None
        and emit exactly two WARN log messages (one per failed attempt).
        """
        target_path = "dbfs:/mnt/silver/events"

        # Build a mock DataFrame whose .write.format().mode().save() chain
        # raises on the first two calls and succeeds on the third.
        mock_df = MagicMock()
        save_mock = mock_df.write.format.return_value.mode.return_value.save
        conflict = _make_conflict_exc("ConcurrentWriteException")
        save_mock.side_effect = [conflict, conflict, None]

        delta_write_with_retry(mock_df, target_path, mode="append", max_retries=3)

        # save() was called exactly 3 times
        self.assertEqual(save_mock.call_count, 3)

        # logger.warning was called at least twice — once per conflict retry
        # (the decorator also calls logger.warning for the sleep notice, so we
        # check that at least 2 calls contain the PATCHIT_EVIDENCE prefix)
        warn_calls = mock_logger.warning.call_args_list
        evidence_calls = [
            c for c in warn_calls if "PATCHIT_EVIDENCE:" in str(c)
        ]
        self.assertEqual(len(evidence_calls), 2)

    # ------------------------------------------------------------------
    # Test 2 — raises RuntimeError after all retries exhausted
    # ------------------------------------------------------------------
    @patch("databricks.delta_utils.time.sleep", return_value=None)
    @patch("databricks.delta_utils.logger")
    def test_raises_after_max_retries(self, mock_logger: MagicMock, mock_sleep: MagicMock) -> None:
        """
        When every attempt raises a conflict error, delta_write_with_retry should
        raise RuntimeError after exactly max_retries attempts.
        """
        target_path = "dbfs:/mnt/gold/orders"
        max_retries = 3

        mock_df = MagicMock()
        save_mock = mock_df.write.format.return_value.mode.return_value.save
        save_mock.side_effect = _make_conflict_exc("DELTA_CONCURRENT_WRITE")

        with self.assertRaises(RuntimeError) as ctx:
            delta_write_with_retry(mock_df, target_path, max_retries=max_retries)

        self.assertIn(f"after {max_retries} retries", str(ctx.exception))
        self.assertIn(target_path, str(ctx.exception))
        self.assertEqual(save_mock.call_count, max_retries)

    # ------------------------------------------------------------------
    # Test 3 — non-conflict errors are NOT retried
    # ------------------------------------------------------------------
    @patch("databricks.delta_utils.time.sleep", return_value=None)
    @patch("databricks.delta_utils.logger")
    def test_no_retry_on_non_conflict_error(self, mock_logger: MagicMock, mock_sleep: MagicMock) -> None:
        """
        A non-conflict error (e.g. PermissionError) should propagate immediately
        without any retry attempt.
        """
        target_path = "dbfs:/mnt/gold/sessions"

        mock_df = MagicMock()
        save_mock = mock_df.write.format.return_value.mode.return_value.save
        save_mock.side_effect = _make_non_conflict_exc()

        with self.assertRaises(PermissionError):
            delta_write_with_retry(mock_df, target_path, max_retries=3)

        # Only one attempt — no retry on non-conflict errors
        self.assertEqual(save_mock.call_count, 1)
        # sleep was never called
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for @delta_conflict_retry decorator
# ---------------------------------------------------------------------------

class TestDeltaConflictRetryDecorator(unittest.TestCase):

    @patch("databricks.delta_utils.time.sleep", return_value=None)
    @patch("databricks.delta_utils.logger")
    def test_decorator_retry_succeeds_after_conflict(
        self, mock_logger: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """Decorated function should succeed if it stops raising before max_retries."""
        call_count = {"n": 0}

        @delta_conflict_retry(max_retries=3)
        def upsert() -> str:
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise _make_conflict_exc("ConcurrentWriteException")
            return "ok"

        result = upsert()
        self.assertEqual(result, "ok")
        self.assertEqual(call_count["n"], 3)

    @patch("databricks.delta_utils.time.sleep", return_value=None)
    @patch("databricks.delta_utils.logger")
    def test_decorator_raises_after_max_retries(
        self, mock_logger: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """Decorated function should raise RuntimeError after max_retries attempts."""
        call_count = {"n": 0}
        max_retries = 3

        @delta_conflict_retry(max_retries=max_retries)
        def always_conflicts() -> None:
            call_count["n"] += 1
            raise _make_conflict_exc("FileAlreadyExistsException")

        with self.assertRaises(RuntimeError) as ctx:
            always_conflicts()

        self.assertIn(f"after {max_retries} retries", str(ctx.exception))
        self.assertEqual(call_count["n"], max_retries)

    @patch("databricks.delta_utils.time.sleep", return_value=None)
    @patch("databricks.delta_utils.logger")
    def test_decorator_no_retry_on_non_conflict(
        self, mock_logger: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """Decorated function should propagate non-conflict errors immediately."""
        call_count = {"n": 0}

        @delta_conflict_retry(max_retries=3)
        def permission_denied() -> None:
            call_count["n"] += 1
            raise PermissionError("Access Denied")

        with self.assertRaises(PermissionError):
            permission_denied()

        self.assertEqual(call_count["n"], 1)
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for PATCHIT_EVIDENCE structured log output
# ---------------------------------------------------------------------------

class TestEvidenceLogFormat(unittest.TestCase):

    @patch("databricks.delta_utils.time.sleep", return_value=None)
    @patch("builtins.print")
    def test_evidence_log_is_valid_json(
        self, mock_print: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """
        Each PATCHIT_EVIDENCE line printed during a conflict retry must be valid JSON
        with the expected keys.
        """
        mock_df = MagicMock()
        save_mock = mock_df.write.format.return_value.mode.return_value.save
        conflict = _make_conflict_exc("ConcurrentWriteException")
        save_mock.side_effect = [conflict, None]

        delta_write_with_retry(mock_df, "dbfs:/mnt/bronze/raw", max_retries=3)

        # Collect all PATCHIT_EVIDENCE print calls
        evidence_lines = [
            args[0]
            for args, _ in mock_print.call_args_list
            if isinstance(args[0], str) and args[0].startswith("PATCHIT_EVIDENCE:")
        ]

        self.assertEqual(len(evidence_lines), 1, "Expected exactly one evidence line for one conflict")

        json_part = evidence_lines[0][len("PATCHIT_EVIDENCE: "):]
        parsed = json.loads(json_part)

        self.assertEqual(parsed["level"], "WARN")
        self.assertEqual(parsed["event"], "delta_write_conflict")
        self.assertIn("attempt", parsed)
        self.assertIn("table", parsed)
        self.assertIn("elapsed_ms", parsed)


if __name__ == "__main__":
    unittest.main()
