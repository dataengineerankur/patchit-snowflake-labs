"""
Delta Lake write-conflict retry utilities for PATCHIT's Databricks pipelines.

PATCHIT uses these helpers to make Delta Lake writes resilient against concurrent
write conflicts that occur when multiple Databricks jobs touch the same Delta table
simultaneously (e.g. streaming micro-batches vs. batch backfills, or multi-cluster
MERGE operations on shared silver/gold tables).

When a conflict is detected, the utilities:
  1. Back off exponentially (2 s → 4 s → 8 s) to let the competing writer finish.
  2. Emit a ``PATCHIT_EVIDENCE:`` prefixed structured JSON log line on every retry
     so PATCHIT's log-fetcher can grep the Databricks driver/executor logs without
     manual log hunting and surface richer incident context in the PATCHIT dashboard.
  3. Re-raise a ``RuntimeError`` with a human-readable enriched message after all
     retries are exhausted, which appears clearly in Databricks job failure emails.

Public API
----------
delta_write_with_retry(df, target_path, mode, max_retries)
    Write a Spark DataFrame to a Delta path with built-in conflict retry.

@delta_conflict_retry(max_retries)
    Decorator for any function that performs DeltaTable merge/update operations.
"""

from __future__ import annotations

import functools
import json
import logging
import time
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exception classification
# ---------------------------------------------------------------------------

_CONFLICT_MARKERS: tuple[str, ...] = (
    "DELTA_CONCURRENT_WRITE",
    "ConcurrentWriteException",
    "FileAlreadyExistsException",
)


def _is_conflict_error(exc: Exception) -> bool:
    """Return True when *exc* represents a Delta write-conflict that is safe to retry."""
    msg = str(exc)
    return any(marker in msg for marker in _CONFLICT_MARKERS)


# ---------------------------------------------------------------------------
# Structured evidence logging
# ---------------------------------------------------------------------------

def _emit_evidence(attempt: int, table: str, elapsed_ms: float) -> None:
    """
    Print a ``PATCHIT_EVIDENCE:`` prefixed JSON log line to stdout **and** the
    Python logger so PATCHIT's grep-based log fetcher picks it up regardless of
    whether the job routes stdout or the logging framework to cloud storage.
    """
    payload = json.dumps(
        {
            "level": "WARN",
            "event": "delta_write_conflict",
            "attempt": attempt,
            "table": table,
            "elapsed_ms": round(elapsed_ms, 2),
        },
        separators=(",", ":"),
    )
    line = f"PATCHIT_EVIDENCE: {payload}"
    print(line, flush=True)
    logger.warning(line)


# ---------------------------------------------------------------------------
# Core retry logic
# ---------------------------------------------------------------------------

_BACKOFF_SECONDS: tuple[float, ...] = (2.0, 4.0, 8.0)


def _backoff_for(attempt: int) -> float:
    """Return the sleep duration (seconds) for the given 0-based *attempt* index."""
    if attempt < len(_BACKOFF_SECONDS):
        return _BACKOFF_SECONDS[attempt]
    return _BACKOFF_SECONDS[-1] * (2 ** (attempt - len(_BACKOFF_SECONDS) + 1))


def delta_write_with_retry(
    df: Any,
    target_path: str,
    mode: str = "append",
    max_retries: int = 3,
) -> None:
    """
    Write a Spark DataFrame to a Delta path, retrying on write-conflict errors.

    Parameters
    ----------
    df:
        A ``pyspark.sql.DataFrame`` (typed as ``Any`` to avoid a PySpark import at
        module level; the function works with any object that exposes
        ``.write.format("delta").mode(mode).save(path)``).
    target_path:
        Absolute DBFS or cloud storage path of the target Delta table
        (e.g. ``"dbfs:/mnt/gold/events"``).
    mode:
        Spark write mode — ``"append"`` (default) or ``"overwrite"``.
    max_retries:
        Maximum number of attempts *including* the first try.  Defaults to 3
        (i.e. up to 2 retries after the initial attempt).

    Raises
    ------
    RuntimeError
        When all *max_retries* attempts are exhausted due to write conflicts.
    Exception
        Any non-conflict exception is re-raised immediately without retrying.
    """
    last_exc: Exception | None = None
    start_ts = time.monotonic()

    for attempt in range(1, max_retries + 1):
        try:
            df.write.format("delta").mode(mode).save(target_path)
            return  # success — exit immediately
        except Exception as exc:  # noqa: BLE001
            if not _is_conflict_error(exc):
                raise  # non-conflict errors are not retried

            elapsed_ms = (time.monotonic() - start_ts) * 1000
            last_exc = exc
            _emit_evidence(attempt, target_path, elapsed_ms)

            if attempt < max_retries:
                sleep_secs = _backoff_for(attempt - 1)
                logger.warning(
                    "delta_write_with_retry: sleeping %.1fs before attempt %d/%d on %s",
                    sleep_secs,
                    attempt + 1,
                    max_retries,
                    target_path,
                )
                time.sleep(sleep_secs)

    raise RuntimeError(
        f"Delta write failed after {max_retries} retries on {target_path}: {last_exc}"
    )


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

F = TypeVar("F", bound=Callable[..., Any])


def delta_conflict_retry(max_retries: int = 3) -> Callable[[F], F]:
    """
    Decorator that wraps a DeltaTable merge/update function with conflict retry logic.

    Usage
    -----
    ::

        @delta_conflict_retry(max_retries=3)
        def upsert_events(delta_table, micro_batch_df, batch_id):
            (
                delta_table.alias("t")
                .merge(micro_batch_df.alias("s"), "t.id = s.id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

    Parameters
    ----------
    max_retries:
        Maximum number of call attempts including the first.  Defaults to 3.

    Returns
    -------
    Callable
        The decorated function with built-in conflict retry and evidence logging.
    """

    def decorator(func: F) -> F:
        table_label = func.__qualname__

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Exception | None = None
            start_ts = time.monotonic()

            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001
                    if not _is_conflict_error(exc):
                        raise

                    elapsed_ms = (time.monotonic() - start_ts) * 1000
                    last_exc = exc
                    _emit_evidence(attempt, table_label, elapsed_ms)

                    if attempt < max_retries:
                        sleep_secs = _backoff_for(attempt - 1)
                        logger.warning(
                            "delta_conflict_retry: sleeping %.1fs before attempt %d/%d on %s",
                            sleep_secs,
                            attempt + 1,
                            max_retries,
                            table_label,
                        )
                        time.sleep(sleep_secs)

            raise RuntimeError(
                f"Delta write failed after {max_retries} retries on {table_label}: {last_exc}"
            )

        return wrapper  # type: ignore[return-value]

    return decorator
