"""Tests for async support — ``async_wait()`` and the ``await task`` protocol."""

import asyncio
from typing import Any

import pytest

from lmttfy.thread import invoke_in_thread, ThreadedCall
from lmttfy.process import invoke_in_sp
from lmttfy.deferred import deferred_call, ExternallyDeferredCall
from lmttfy.task import LMTTask
from lmttfy.exceptions import BurstWhileNoTaskErrorsException

from tests.helpers import sleep_n_add, raise_value_error


# ---------------------------------------------------------------------------
# LMTTask async_wait and __await__
# ---------------------------------------------------------------------------

class TestLMTTaskAsync:
    """async_wait() and __await__ protocol on LMTTask."""

    @pytest.mark.anyio
    async def test_async_wait_threaded(self):
        """async_wait() on a ThreadedCall-based task returns the correct value."""
        f = invoke_in_thread()(sleep_n_add)
        result = f(0.05, 1, 2)
        val = await result.async_wait()
        assert val == 3

    @pytest.mark.anyio
    async def test_await_protocol_threaded(self):
        """``await task`` works for ThreadedCall-based tasks."""
        f = invoke_in_thread()(sleep_n_add)
        result = f(0.05, 10, 20)
        val = await result
        assert val == 30

    @pytest.mark.anyio
    async def test_async_wait_subprocess(self):
        """async_wait() on a MultiProcessedCall-based task."""
        f = invoke_in_sp()(sleep_n_add)
        result = f(0.05, 100, 200)
        val = await result.async_wait()
        assert val == 300

    @pytest.mark.anyio
    async def test_await_protocol_subprocess(self):
        """``await task`` works for MultiProcessedCall-based tasks."""
        f = invoke_in_sp()(sleep_n_add)
        result = f(0.05, 3, 4)
        val = await result
        assert val == 7

    @pytest.mark.anyio
    async def test_async_wait_deferred_local(self):
        """async_wait() on a deferred (local fallback) task."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 5, 6)
        val = await result.async_wait()
        assert val == 11

    @pytest.mark.anyio
    async def test_await_protocol_deferred_local(self):
        """``await task`` works for deferred local-fallback tasks."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 7, 8)
        val = await result
        assert val == 15


# ---------------------------------------------------------------------------
# async_wait timeout
# ---------------------------------------------------------------------------

class TestAsyncWaitTimeout:
    """Timeouts in async_wait."""

    @pytest.mark.anyio
    async def test_timeout_expires_thread(self):
        """async_wait(timeout=0.01) returns None when the thread is still running."""
        f = invoke_in_thread()(sleep_n_add)
        result = f(2, 1, 2)
        val = await result.async_wait(timeout=0.01)
        assert val is None
        # subsequent wait gets the value
        val = await result.async_wait()
        assert val == 3

    @pytest.mark.anyio
    async def test_timeout_expires_subprocess(self):
        """async_wait(timeout=0.01) on a subprocess returns None when still running."""
        f = invoke_in_sp()(sleep_n_add)
        result = f(2, 1, 2)
        val = await result.async_wait(timeout=0.01)
        assert val is None
        val = await result.async_wait()
        assert val == 3

    @pytest.mark.anyio
    async def test_timeout_expires_deferred_local(self):
        """async_wait(timeout=0.01) on a deferred local-fallback task."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(2, 1, 2)
        val = await result.async_wait(timeout=0.01)
        assert val is None
        val = await result.async_wait()
        assert val == 3


# ---------------------------------------------------------------------------
# async_wait error handling
# ---------------------------------------------------------------------------

class TestAsyncWaitErrors:
    """Errors in async_wait should not raise on wait, only on burst."""

    @pytest.mark.anyio
    async def test_error_does_not_raise_on_wait(self):
        """async_wait() returns None (not raises) when the function raises,
        matching the sync wait() behaviour."""
        f = deferred_call(allow_local=True)(raise_value_error)
        result = f("async_err")
        val = await result.async_wait()
        assert val is None

    @pytest.mark.anyio
    async def test_burst_raises_after_async_wait(self):
        """burst() raises the captured exception after async_wait."""
        f = invoke_in_thread()(raise_value_error)
        result = f("burst_after_async")
        await result.async_wait()
        with pytest.raises(ValueError, match="burst_after_async"):
            result.burst()


# ---------------------------------------------------------------------------
# concurrent async tasks (no event-loop blocking)
# ---------------------------------------------------------------------------

class TestAsyncConcurrency:
    """Multiple concurrent async_wait calls do not block each other."""

    @pytest.mark.anyio
    async def test_concurrent_async_waits(self):
        """Several async_wait calls on tasks with staggered sleep complete
        in roughly the expected total wall time (should be ~max, not ~sum)."""
        f = invoke_in_thread()(sleep_n_add)
        t1 = f(0.3, 1, 2)
        t2 = f(0.3, 3, 4)
        t3 = f(0.3, 5, 6)

        import time
        start = time.monotonic()
        r1, r2, r3 = await asyncio.gather(
            t1.async_wait(), t2.async_wait(), t3.async_wait()
        )
        elapsed = time.monotonic() - start

        assert r1 == 3
        assert r2 == 7
        assert r3 == 11
        # should take ~0.3s, not ~0.9s — allow some margin
        assert elapsed < 0.5, f"took {elapsed:.2f}s, expected <0.5s"


# ---------------------------------------------------------------------------
# event loop is not blocked during async_wait
# ---------------------------------------------------------------------------

class TestEventLoopNotBlocked:
    """Async operations on the event loop continue running during async_wait."""

    @pytest.mark.anyio
    async def test_event_loop_stays_responsive(self):
        """A background asyncio task progresses while async_wait is running."""
        f = invoke_in_thread()(sleep_n_add)
        result = f(0.3, 1, 2)

        progress = []
        async def tracker():
            for _ in range(5):
                await asyncio.sleep(0.05)
                progress.append("tick")

        done, _ = await asyncio.gather(
            result.async_wait(),
            tracker(),
            return_exceptions=False,
        )

        assert done == 3
        # the tracker should have run several times during the 0.3s wait
        assert len(progress) >= 3, f"only {len(progress)} ticks, expected >=3"
