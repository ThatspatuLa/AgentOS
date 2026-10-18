"""Authenticated cross-process quiescence lease for one Nami state root.

This lease currently fences only writers explicitly routed through it. It is
not a claim that all Nami stores or arbitrary processes are quiesced.
"""
from __future__ import annotations

import os
import re
import stat
import threading
import uuid
from pathlib import Path

from engine.lib import nami_io


class ExactRootLeaseError(RuntimeError):
    """An exact-root lease could not be safely established or verified."""


_REPARSE = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
_LOCK_RELATIVE = ".nami-raw-dataset-write-gate.lock"


def _resolve_plain_root(value: Path | str) -> Path:
    lexical = Path(value).expanduser()
    if ".." in lexical.parts:
        raise ExactRootLeaseError("exact_root_traversal_component")
    lexical = lexical.absolute()
    current = Path(lexical.anchor)
    for component in lexical.parts[1:]:
        current /= component
        try:
            info = current.lstat()
        except OSError as exc:
            raise ExactRootLeaseError("exact_root_component_unavailable") from exc
        if stat.S_ISLNK(info.st_mode) or bool(getattr(info, "st_file_attributes", 0) & _REPARSE):
            raise ExactRootLeaseError("exact_root_reparse_point")
    try:
        resolved = lexical.resolve(strict=True)
        info = resolved.lstat()
    except (OSError, RuntimeError) as exc:
        raise ExactRootLeaseError("exact_root_unavailable") from exc
    if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode) or bool(
            getattr(info, "st_file_attributes", 0) & _REPARSE):
        raise ExactRootLeaseError("exact_root_type_invalid")
    return resolved


class _Attestation:
    __slots__ = ("owner", "root", "transaction_id", "nonce")

    def __init__(self, owner, root: Path, transaction_id: str):
        self.owner = owner
        self.root = root
        self.transaction_id = transaction_id
        self.nonce = os.urandom(32)


class ExactRootQuiescenceLease:
    """Exclusive root-bound lease with an unforgeable in-process capability.

    The Windows backend pins the state-root handle and acquires the lock via
    that retained handle, rejecting symlink/reparse ancestors. Other platforms
    fail closed because the current Nami writer boundary relies on the Windows
    retained-handle namespace guarantees.
    """

    def __init__(self, root: Path | str, transaction_id: str | None = None):
        self.root = _resolve_plain_root(root)
        self.transaction_id = transaction_id or uuid.uuid4().hex
        if not isinstance(self.transaction_id, str) or not re.fullmatch(r"[0-9a-f]{32}", self.transaction_id):
            raise ExactRootLeaseError("exact_root_transaction_id_invalid")
        self._secure_root = None
        self._root_identity = None
        self._context = None
        self._active_thread = None
        self._gate_identity = None
        self._attestation = _Attestation(self, self.root, self.transaction_id)
        if os.name != "nt":
            raise ExactRootLeaseError("exact_root_retained_handle_backend_unavailable")
        try:
            from engine.lib.secure_state_io import SecureStateRoot
            self._secure_root = SecureStateRoot(root, writer_root=True)
            final = self._secure_root.root_final.removeprefix("\\\\?\\").replace("/", "\\")
            if final.casefold() != str(self.root).casefold():
                raise ExactRootLeaseError("exact_root_handle_path_mismatch")
            self._root_identity = self._secure_root.identity()
        except BaseException:
            self.close()
            raise

    @property
    def attestation(self):
        return self._attestation

    def __enter__(self):
        if self._secure_root is None:
            raise ExactRootLeaseError("exact_root_handle_unavailable")
        try:
            self._context = nami_io.file_lock_on_secure_root(
                self._secure_root, _LOCK_RELATIVE, create=True)
            lock_fd = self._context.__enter__()
            import msvcrt
            self._gate_identity = self._secure_root.handle_identity(msvcrt.get_osfhandle(lock_fd))
            self._active_thread = threading.get_ident()
            self.assert_bound_root(self.root)
            return self
        except BaseException:
            self._active_thread = None
            self._gate_identity = None
            context, self._context = self._context, None
            if context is not None:
                context.__exit__(None, None, None)
            self.close()
            raise

    def __exit__(self, exc_type, exc, tb):
        self._active_thread = None
        self._gate_identity = None
        context, self._context = self._context, None
        result = context.__exit__(exc_type, exc, tb) if context is not None else False
        self.close()
        return result

    def close(self):
        if self._secure_root is not None:
            self._secure_root.close()
            self._secure_root = None

    def __del__(self):
        if getattr(self, "_secure_root", None) is not None:
            self._secure_root.close()
            self._secure_root = None

    def assert_bound_root(self, root):
        try:
            resolved = _resolve_plain_root(root)
            identity = self._secure_root.identity() if self._secure_root is not None else None
        except Exception as exc:
            raise ExactRootLeaseError("exact_root_lease_identity_unavailable") from exc
        if (self._active_thread != threading.get_ident() or resolved != self.root
                or identity != self._root_identity or self._context is None
                or self._gate_identity is None):
            raise ExactRootLeaseError("exact_root_lease_not_active_for_root")

    def verify_quiescence_attestation(self, token, root, transaction_id):
        self.assert_bound_root(root)
        return (token is self._attestation and token.owner is self
                and token.root == self.root and token.transaction_id == transaction_id
                and transaction_id == self.transaction_id)

    def ensure_directory(self, relative):
        """Create/check a relative directory one no-follow component at a time."""
        self.assert_bound_root(self.root)
        from engine.lib.secure_state_io import _CloseHandle
        parts = self._secure_root._parts(str(relative).replace("\\", "/"))
        for index in range(1, len(parts) + 1):
            handle, parents = self._secure_root.open_handle(
                "/".join(parts[:index]), directory=True, writable=True, create=True)
            _CloseHandle(handle)
            for parent in reversed(parents):
                _CloseHandle(parent)

    def assert_safe_file_or_missing(self, relative):
        """Reject a linked/special existing destination beneath the pinned root."""
        self.assert_bound_root(self.root)
        from engine.lib.secure_state_io import SecureStateIOError, _CloseHandle
        relative = str(relative).replace("\\", "/")
        parts = self._secure_root._parts(relative)
        try:
            handle, parents = self._secure_root.open_handle(
                relative, directory=False, writable=False, create=False)
        except SecureStateIOError as exc:
            # Missing leaf is allowed for create-only writers; missing parent
            # is not. Verify the complete parent chain separately.
            if getattr(exc, "errno", None) not in {2, 3} and "file not found" not in str(exc).lower():
                raise ExactRootLeaseError("exact_root_child_path_invalid") from exc
            if len(parts) > 1:
                parent, opened = self._secure_root.open_handle(
                    "/".join(parts[:-1]), directory=True, writable=False, create=False)
                _CloseHandle(parent)
                for item in reversed(opened):
                    _CloseHandle(item)
            return
        try:
            identity = self._secure_root.handle_identity(handle)
            if identity["links"] != 1:
                raise ExactRootLeaseError("exact_root_child_link_count_invalid")
        finally:
            _CloseHandle(handle)
            for item in reversed(parents):
                _CloseHandle(item)
