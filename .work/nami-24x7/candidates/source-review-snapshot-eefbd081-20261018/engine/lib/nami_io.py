"""
nami_io.py â€” file I/O helpers for Nami cron jobs (and engine).

Centralizes the safe-append pattern so all writers (engine + crons) use
the same flock-protected atomic writes. Avoids race conditions where the
engine and a cron step on each other's writes.

Usage from a cron:
  from engine.lib.nami_io import safe_append_dead_end, safe_append_promoted,
                                   safe_mark_hypothesis_researched
  safe_append_dead_end({...})
"""

from __future__ import annotations

import json
import errno
import logging
import os
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from engine import config  # noqa: E402

logger = logging.getLogger("nami.io")


class JsonCorruptionError(RuntimeError):
    """A JSON artifact exists but cannot be safely read or mutated."""


# Cross-process file lock.
#
# - On Linux/macOS: fcntl.flock on a sidecar .lock file.
# - On Windows: msvcrt.locking on the same sidecar file. The thread lock is
#   only an in-process fast path; it is not the correctness boundary.
_PROCESS_LOCKS: dict[str, "threading.Lock"] = {}


def _flock_available() -> bool:
    """fcntl is only on Linux/macOS. Cached at module level."""
    try:
        import fcntl  # noqa: F401
        return True
    except ImportError:
        return False


_HAS_FCNTL = _flock_available()


def _lock_windows_byte(lf, msvcrt_module, *, timeout=60.0):
    """Wait for a Windows byte-range lock without msvcrt's short retry limit.

    LK_LOCK gives up after a small fixed number of retries. Cron workers often
    contend briefly during startup, so use non-blocking attempts and retry only
    documented lock-contention errors until the bounded deadline.
    """
    deadline = time.monotonic() + timeout
    delay = 0.01
    contention = {errno.EACCES, errno.EAGAIN, errno.EDEADLK, errno.EWOULDBLOCK}
    while True:
        try:
            lf.seek(0)
            msvcrt_module.locking(lf.fileno(), msvcrt_module.LK_NBLCK, 1)
            return
        except OSError as exc:
            if exc.errno not in contention or time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 0.1)


@contextmanager
def file_lock(path: Path):
    """Acquire an advisory lock that works across Windows processes."""
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    key = str(lock_path.resolve())
    plock = _PROCESS_LOCKS.setdefault(key, threading.Lock())
    plock.acquire()
    try:
        lf = open(lock_path, "a+b")
    except BaseException:
        plock.release()
        raise
    try:
        if _HAS_FCNTL:
            import fcntl
            fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
        else:
            import msvcrt
            lf.seek(0, os.SEEK_END)
            if lf.tell() == 0:
                lf.write(b"0")
                lf.flush()
            _lock_windows_byte(lf, msvcrt)
        yield lf
    finally:
        if _HAS_FCNTL:
            import fcntl
            try:
                fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        else:
            import msvcrt
            try:
                lf.seek(0)
                msvcrt.locking(lf.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        lf.close()
        plock.release()


@contextmanager
def file_lock_under_root(state_root: Path, relative_lock_path: str, *, expected_identity=None):
    """Acquire a sidecar lock through the secure Windows state-root handle."""
    if _HAS_FCNTL:
        lock_path = Path(state_root) / relative_lock_path
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        key = str(lock_path.absolute())
        plock = _PROCESS_LOCKS.setdefault(key, threading.Lock())
        plock.acquire()
        try:
            with open(lock_path, "a+b") as lf:
                import fcntl
                fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
                try:
                    yield lf
                finally:
                    fcntl.flock(lf.fileno(), fcntl.LOCK_UN)
        finally:
            plock.release()
        return

    from engine.lib.secure_state_io import SecureStateRoot
    key = str(Path(state_root).absolute() / relative_lock_path).casefold()
    plock = _PROCESS_LOCKS.setdefault(key, threading.Lock())
    plock.acquire()
    secure = None
    fd = None
    parents = []
    lock_file = None
    try:
        deadline = time.monotonic() + 60.0
        while True:
            try:
                secure = SecureStateRoot(state_root, writer_root=True)
                break
            except OSError as exc:
                if getattr(exc, "errno", None) not in {32, 33} or time.monotonic() >= deadline:
                    raise
                time.sleep(0.025)
        if expected_identity is not None and secure.identity() != expected_identity:
            raise OSError("secure_lock_root_identity_mismatch")
        with file_lock_on_secure_root(secure, relative_lock_path, create=True) as lock_fd:
            yield lock_fd
    finally:
        if lock_file is not None:
            lock_file.close()
        if fd is not None:
            import msvcrt
            try:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
            os.close(fd)
        for handle in reversed(parents):
            from engine.lib.secure_state_io import _CloseHandle
            _CloseHandle(handle)
        if secure is not None:
            secure.close()
        plock.release()


@contextmanager
def file_lock_on_secure_root(secure_root, relative_lock_path: str, *, create=False):
    """Lock one sidecar through an already retained SecureStateRoot handle."""
    if _HAS_FCNTL:
        raise OSError("secure_root_relative_lock_unavailable_on_posix")
    key = (str(secure_root.path) + "/" + relative_lock_path).casefold()
    plock = _PROCESS_LOCKS.setdefault(key, threading.Lock())
    plock.acquire()
    fd = None
    parents = []
    lock_file = None
    try:
        fd, parents = secure_root.lock(relative_lock_path, create=create)
        # Lock before inspecting the gate byte. A concurrent process already
        # holding this Windows byte range can make pre-lock reads fail with
        # PermissionError (and, more importantly, inspection must be serialized
        # with empty-file initialization).
        lock_file = os.fdopen(fd, "r+b", closefd=False)
        _lock_windows_byte(lock_file, __import__("msvcrt"))
        size = os.fstat(fd).st_size
        os.lseek(fd, 0, os.SEEK_SET)
        current = os.read(fd, 8)
        if size == 0 and create:
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, b"0")
            os.fsync(fd)
        elif current != b"0":
            raise OSError("secure_writer_gate_bytes_invalid")
        os.lseek(fd, 0, os.SEEK_SET)
        yield fd
    finally:
        if lock_file is not None:
            lock_file.close()
        if fd is not None:
            import msvcrt
            try:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
            os.close(fd)
        for handle in reversed(parents):
            from engine.lib.secure_state_io import _CloseHandle
            _CloseHandle(handle)
        plock.release()

def _atomic_replace(src: Path, dst: Path) -> None:
    """Atomic rename. Linux Path.rename is atomic-replace; Windows refuses
    if dst exists. os.replace() handles both cases."""
    os.replace(src, dst)


def safe_read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("safe_read_json: %s unreadable; refusing fallback mutation: %s", path, exc)
        raise JsonCorruptionError(f"unreadable JSON artifact: {path}") from exc


def safe_append_json(path: Path, record) -> bool:
    """Atomically append a record to a JSON array file. Returns True on success.

    IMPORTANT: caller MUST already hold `file_lock(path)` to be safe.
    The function does NOT acquire the lock itself â€” that's by design to
    avoid recursive-lock deadlocks (Linux flock(2) on a different fd in
    the same process blocks indefinitely).
    """
    try:
        existing = safe_read_json(path, [])
        if not isinstance(existing, list):
            logger.error("safe_append_json: %s is not a JSON array, refusing to overwrite", path)
            return False
        existing.append(record)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(existing, indent=2, default=str))
        _atomic_replace(tmp_path, path)
        return True
    except Exception as exc:
        logger.error("safe_append_json: %s failed: %s", path, exc, exc_info=True)
        return False


def safe_append_jsonl(path: Path, record: dict) -> bool:
    """Atomically append a single JSON record as a line to a JSONL file.

    JSONL is ideal for append-only streams: no read-modify-write race,
    each line is independent, easy to count lines / filter / parse.

    IMPORTANT: caller MUST already hold `file_lock(path)` to be safe.

    Returns True on success, False on error.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(record, default=str)
        # Atomic append: write to temp then rename-or-append. For pure append
        # we open in 'a' mode with explicit lock context.
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        return True
    except Exception as exc:
        logger.error("safe_append_jsonl: %s failed: %s", path, exc, exc_info=True)
        return False


def safe_write_json(path: Path, payload) -> bool:
    """Atomic full-write. IMPORTANT: caller MUST already hold `file_lock(path)`."""
    try:
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, default=str))
        _atomic_replace(tmp_path, path)
        return True
    except Exception as exc:
        logger.error("safe_write_json: %s failed: %s", path, exc, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# High-level helpers used by the research-executor cron
# ---------------------------------------------------------------------------
def safe_append_dead_end(record: dict[str, Any]) -> bool:
    """Append a dead-end record. Auto-fills market + edge_type from hypothesis_queue if missing."""
    config.DEAD_ENDS.parent.mkdir(parents=True, exist_ok=True)

    # Backfill market + edge_type from hypothesis_queue if not present
    if "market" not in record or "edge_type" not in record:
        hid = record.get("hypothesis_id")
        if hid:
            queue = safe_read_json(config.STATE_DIR / "hypothesis_queue.json", [])
            for h in queue:
                if h.get("id") == hid:
                    record.setdefault("market", h.get("market"))
                    record.setdefault("edge_type", h.get("edge_type"))
                    record.setdefault("source", h.get("source"))
                    break

    record.setdefault("killed_at", datetime.now(timezone.utc).isoformat())
    try:
        with file_lock(config.DEAD_ENDS):
            return safe_append_json(config.DEAD_ENDS, record)
    except Exception as exc:
        logger.error("safe_append_dead_end: %s", exc, exc_info=True)
        return False


def safe_append_promoted(record: dict[str, Any]) -> bool:
    promoted_path = config.STATE_DIR / "promoted.json"
    promoted_path.parent.mkdir(parents=True, exist_ok=True)
    record.setdefault("promoted_at", datetime.now(timezone.utc).isoformat())
    try:
        with file_lock(promoted_path):
            return safe_append_json(promoted_path, record)
    except Exception as exc:
        logger.error("safe_append_promoted: %s", exc, exc_info=True)
        return False


def safe_append_candidate(record: dict[str, Any]) -> bool:
    """Used by research-executor to add a candidate spec to the queue.

    DEDUPLICATES:
      1. By hypothesis_id (cheap ID-level dedup; existing behavior).
      2. By wallet-driven dedup key: (structured_data.wallet_address,
         structured_data.signal_type, structured_data.outcome). If a
         candidate already exists for that wallet+signal_type+outcome
         triple, the new one is merged in (we keep the older candidate
         and update its aggregated fields like latest_trade_ts).

    The wallet dedup collapses the 18x duplication we saw on a single
    wallet's trades â€” each triggering trade was generating a fresh
    candidate. With this fix, the second candidate for the same wallet
    just updates the existing one with the new trade's timestamp.
    """
    cand_path = config.STATE_DIR / "candidate_queue.json"
    cand_path.parent.mkdir(parents=True, exist_ok=True)
    record.setdefault("researched_at", datetime.now(timezone.utc).isoformat())

    new_id = record.get("hypothesis_id")
    new_sd = record.get("structured_data") or {}
    new_wallet = new_sd.get("wallet_address") or ""
    new_sig_type = new_sd.get("signal_type") or ""
    new_outcome = new_sd.get("outcome") or ""
    new_ts = new_sd.get("timestamp") or 0

    def _wallet_key(sd: dict) -> tuple[str, str, str]:
        return (
            sd.get("wallet_address") or "",
            sd.get("signal_type") or "",
            sd.get("outcome") or "",
        )

    try:
        with file_lock(cand_path):
            existing = safe_read_json(cand_path, [])
            if not isinstance(existing, list):
                existing = []

            # Dedup 1: by hypothesis_id (existing behavior)
            if new_id and any(c.get("hypothesis_id") == new_id for c in existing):
                logger.info(
                    "safe_append_candidate: hypothesis_id=%s already in queue, skipping",
                    new_id,
                )
                return False

            # Dedup 2: by wallet+signal_type+outcome
            if new_wallet and new_sig_type:
                new_key = _wallet_key(new_sd)
                for c in existing:
                    c_sd = c.get("structured_data") or {}
                    if _wallet_key(c_sd) == new_key:
                        # Found a duplicate wallet-driven candidate. Merge
                        # instead of appending.
                        prev_ts = c_sd.get("timestamp") or 0
                        if new_ts and new_ts > prev_ts:
                            c_sd["timestamp"] = new_ts
                            c_sd["latest_trade_ts"] = new_ts
                            # Bump seen_trades count if present
                            c_sd["seen_trades"] = int(c_sd.get("seen_trades") or 1) + 1
                            c["researched_at"] = record.get("researched_at") or c.get("researched_at")
                            # Carry forward wallet_pnl verdict from new record
                            for k in (
                                "wallet_pnl_verdict",
                                "wallet_pnl_win_rate",
                                "wallet_pnl_n_trades",
                                "wallet_pnl_n_resolved",
                                "wallet_pnl_n_won",
                                "wallet_pnl_reason",
                            ):
                                if k in new_sd:
                                    c_sd[k] = new_sd[k]
                        tmp_path = cand_path.with_suffix(cand_path.suffix + ".tmp")
                        tmp_path.write_text(json.dumps(existing, indent=2, default=str))
                        _atomic_replace(tmp_path, cand_path)
                        logger.info(
                            "safe_append_candidate: merged wallet=%s sig=%s into existing candidate",
                            new_wallet[:12], new_sig_type,
                        )
                        return True  # treated as success (not a duplicate failure)

            return safe_append_json(cand_path, record)
    except Exception as exc:
        logger.error("safe_append_candidate: %s", exc, exc_info=True)
        return False


def safe_remove_processed_candidates(processed_ids: list[str]) -> int:
    """Remove processed (evaluated) candidates from candidate_queue.json.

    Called by the engine after a backtest cycle to keep the queue bounded.
    Returns the number of candidates removed.
    """
    if not processed_ids:
        return 0
    cand_path = config.STATE_DIR / "candidate_queue.json"
    try:
        with file_lock(cand_path):
            existing = safe_read_json(cand_path, [])
            before = len(existing)
            remaining = [c for c in existing if c.get("hypothesis_id") not in processed_ids]
            removed = before - len(remaining)
            if removed > 0:
                tmp_path = cand_path.with_suffix(cand_path.suffix + ".tmp")
                tmp_path.write_text(json.dumps(remaining, indent=2, default=str))
                _atomic_replace(tmp_path, cand_path)
            return removed
    except Exception as exc:
        logger.error("safe_remove_processed_candidates: %s", exc, exc_info=True)
        return 0


def safe_mark_hypothesis_status(hypothesis_id: str, new_status: str,
                                extra_fields: dict[str, Any] | None = None) -> bool:
    """Mark a hypothesis in the queue with a new status (and optional extras).

    Used by research-executor when picking up / finishing a hypothesis.
    """
    queue_path = config.STATE_DIR / "hypothesis_queue.json"
    try:
        with file_lock(queue_path):
            queue = safe_read_json(queue_path, [])
            found = False
            for h in queue:
                if h.get("id") == hypothesis_id:
                    h["status"] = new_status
                    if extra_fields:
                        h.update(extra_fields)
                    h["last_updated_at"] = datetime.now(timezone.utc).isoformat()
                    found = True
                    break
            if not found:
                logger.warning("safe_mark_hypothesis_status: %s not found in queue", hypothesis_id)
                return False
            tmp_path = queue_path.with_suffix(queue_path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(queue, indent=2, default=str))
            _atomic_replace(tmp_path, queue_path)
        return True
    except Exception as exc:
        logger.error("safe_mark_hypothesis_status: %s failed: %s", hypothesis_id, exc, exc_info=True)
        return False


def safe_pick_pending_hypotheses(n: int) -> list[dict[str, Any]]:
    """Atomically mark up to N pending hypotheses as in_research and return them."""
    queue_path = config.STATE_DIR / "hypothesis_queue.json"
    picked = []
    try:
        with file_lock(queue_path):
            queue = safe_read_json(queue_path, [])
            now = datetime.now(timezone.utc).isoformat()
            pending = [h for h in queue if h.get("status") == "queued"]
            for h in pending[:n]:
                h["status"] = "in_research"
                h["research_started_at"] = now
                picked.append(h)
            tmp_path = queue_path.with_suffix(queue_path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(queue, indent=2, default=str))
            _atomic_replace(tmp_path, queue_path)
        return picked
    except Exception as exc:
        logger.error("safe_pick_pending_hypotheses: %s", exc, exc_info=True)
        return []
