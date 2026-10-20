"""Named crash points for failure-injection tests.

Production code calls ``hit("name")`` at the boundaries of each crash window. Normally that is a
no-op. A test (or ``TELEMETRY_FAILPOINT=name[:nth]`` for a subprocess) arms a point:

* ``raise`` mode raises `SimulatedCrash` (a BaseException, so no ``except Exception`` can swallow
  it) - fast, in-process.
* ``exit`` mode calls ``os._exit(137)``: no cleanup, no ``finally``, no SQLite rollback code - the
  closest thing to ``kill -9`` that is reproducible. Used by the real-process-kill tests.

Points, in protocol order (see docs/recovery.md for the window each one models):

    before_stage      window 1  nothing written yet
    write_tmp_partial window 2  half of a Parquet file is on disk under a temp name
    after_stage       window 3  files staged and recorded, not yet placed
    after_place       window 3  files placed in data/, manifest still says VERIFIED
    in_commit         window 4  inside the metadata transaction, before COMMIT
    after_commit      window 5  committed; producer has not been told
    recover_*         crashes *inside* recovery (recovery must itself be re-runnable)
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

POINTS = (
    "before_stage",
    "write_tmp_partial",
    "after_stage",
    "after_place",
    "in_commit",
    "after_commit",
    "recover_before_commit",
    "recover_after_interrupt",
)


class SimulatedCrash(BaseException):
    """Raised at an armed failpoint in ``raise`` mode."""

    def __init__(self, point: str) -> None:
        super().__init__(f"simulated crash at {point}")
        self.point = point


_armed: dict[str, tuple[str, int]] = {}  # point -> (mode, remaining hits before firing)


def arm(point: str, mode: str = "raise", nth: int = 1) -> None:
    if point not in POINTS:
        raise ValueError(f"unknown failpoint {point!r}; known: {', '.join(POINTS)}")
    if mode not in ("raise", "exit"):
        raise ValueError("mode must be 'raise' or 'exit'")
    _armed[point] = (mode, nth)


def disarm() -> None:
    _armed.clear()


@contextmanager
def armed(point: str, mode: str = "raise", nth: int = 1) -> Iterator[None]:
    arm(point, mode, nth)
    try:
        yield
    finally:
        disarm()


def load_from_env() -> None:
    """Arm from ``TELEMETRY_FAILPOINT=name[:nth]`` (always ``exit`` mode)."""
    spec = os.environ.get("TELEMETRY_FAILPOINT")
    if spec:
        name, _, nth = spec.partition(":")
        arm(name, "exit", int(nth) if nth else 1)


def hit(point: str) -> None:
    entry = _armed.get(point)
    if entry is None:
        return
    mode, remaining = entry
    if remaining > 1:
        _armed[point] = (mode, remaining - 1)
        return
    del _armed[point]
    if mode == "exit":
        os._exit(137)
    raise SimulatedCrash(point)
