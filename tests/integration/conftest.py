"""Path setup + helpers for the rayx.runtime INTEGRATION tests.

Unlike tests/unit (pure, import-light), these tests import ``rayx.runtime`` and so
require the built ``_rayx`` extension + HPX. Each test module does
``pytest.importorskip("rayx.runtime")`` so the suite **skips cleanly** on an unbuilt
checkout (missing ``_rayx`` -> ImportError -> module-level skip).

This conftest only puts ``python/src`` on ``sys.path`` (so ``import rayx`` resolves)
and exposes a ``wait_until`` helper. It deliberately does NOT import ``rayx`` at
module load, so collecting this directory never errors on an unbuilt checkout. Tests
construct their own ``Runtime(...)`` (configs differ per test, and ``Runtime`` is a
process-singleton) rather than sharing one fixture.
"""
import pathlib
import sys
import time

import pytest

_SRC = pathlib.Path(__file__).resolve().parents[2] / "python" / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


@pytest.fixture
def wait_until():
    """Poll a predicate (e.g. a lane_stats state) instead of sleeping a guess.

    Keeps the cancellation / admission tests deterministic without wall-clock
    guesses; fails the test if the predicate stays false past the timeout.
    """
    def _wait(pred, timeout_s=5.0, where="condition"):
        deadline = time.perf_counter() + timeout_s
        while time.perf_counter() < deadline:
            if pred():
                return
            time.sleep(0.001)
        raise AssertionError(f"timed out waiting for {where}")
    return _wait
