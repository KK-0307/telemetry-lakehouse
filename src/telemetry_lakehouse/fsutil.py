"""Filesystem primitives that make 'immutable, never overwritten' enforceable."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path

from telemetry_lakehouse import failpoints


class ImmutableFileExistsError(FileExistsError):
    """An attempt was made to create a file at a path that already exists."""


def fsync_dir(path: Path) -> None:
    """Persist directory entries (new files, renames) to stable storage."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_new_file(path: Path, data: bytes | memoryview) -> None:
    """Durably create `path` with `data`; fail (never replace) if it already exists.

    The bytes go to a hidden temp file in the same directory, are fsynced, then hard-linked
    to the final name. `link(2)` fails atomically if the target exists, which `rename(2)`
    would not. Readers therefore never observe a partially written file under `path`.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with tmp.open("xb") as fh:
            view = memoryview(data)
            half = len(view) // 2
            fh.write(view[:half])
            fh.flush()
            failpoints.hit("write_tmp_partial")  # a torn write: half the bytes, temp name
            fh.write(view[half:])
            fh.flush()
            os.fsync(fh.fileno())
        try:
            os.link(tmp, path)
        except FileExistsError as exc:
            raise ImmutableFileExistsError(f"refusing to overwrite {path}") from exc
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()
    fsync_dir(path.parent)
