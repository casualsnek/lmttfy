"""Tests for decorating ``async def`` functions with all three backends."""

import asyncio
from typing import Any

import pytest

from lmttfy.thread import invoke_in_thread, ThreadedCall
from lmttfy.process import invoke_in_sp
from lmttfy.deferred import deferred_call, ExternallyDeferredCall
from lmttfy.task import LMTTask
from lmttfy.exceptions import BurstWhileNoTaskErrorsException

from tests.helpers import async_sleep_n_add, async_raise_value_error


# ---------------------------------------------------------------------------
# invoke_in_thread with async functions
# ---------------------------------------------------------------------------

class TestThreadAsyncFunction:
    """@invoke_in_thread works with async def functions."""

    def test_sync_wait(self):
        """Sync wait() returns the result of an async function."""
        f = invoke_in_thread()(async_sleep_n_add)
        result = f(0.05, 1, 2)
        assert result.wait() == 3

    def test_multiple_concurrent(self):
        """Multiple concurrent async calls via thread all resolve."""
        f = invoke_in_thread()(async_sleep_n_add)
        calls = [f(0.1, i, i * 10) for i in range(5)]
        for i, c in enumerate(calls):
            assert c.wait() == i + i * 10, f"call {i} mismatch"

    def test_error_captured(self):
        """Error from an async function is captured and re-raised via burst()."""
        f = invoke_in_thread()(async_raise_value_error)
        result = f("async_err")
        result.wait()
        with pytest.raises(ValueError, match="async_err"):
            result.burst()


# ---------------------------------------------------------------------------
# invoke_in_sp with async functions
# ---------------------------------------------------------------------------

class TestSubprocessAsyncFunction:
    """@invoke_in_sp works with async def functions."""

    def test_sync_wait(self):
        """Sync wait() returns the result of an async function in a subprocess."""
        f = invoke_in_sp()(async_sleep_n_add)
        result = f(0.05, 10, 20)
        assert result.wait() == 30

    def test_error_captured(self):
        """Error from an async function in a subprocess is propagated."""
        f = invoke_in_sp()(async_raise_value_error)
        result = f("sp_async_err")
        result.wait()
        with pytest.raises(ValueError, match="sp_async_err"):
            result.burst()


# ---------------------------------------------------------------------------
# deferred_call with async functions (local fallback)
# ---------------------------------------------------------------------------

class TestDeferredAsyncFunction:
    """@deferred_call works with async def functions (local fallback)."""

    def test_sync_wait(self):
        """Sync wait() returns the result of an async function via deferred."""
        f = deferred_call(allow_local=True)(async_sleep_n_add)
        result = f(0.05, 100, 200)
        assert result.wait() == 300

    def test_error_captured(self):
        """Error from an async deferred function is captured."""
        f = deferred_call(allow_local=True)(async_raise_value_error)
        result = f("deferred_async_err")
        result.wait()
        with pytest.raises(ValueError, match="deferred_async_err"):
            result.burst()


# ---------------------------------------------------------------------------
# async_wait on results of async functions
# ---------------------------------------------------------------------------

class TestAsyncFunctionAsyncWait:
    """async_wait() / await on tasks returned from async functions."""

    @pytest.mark.anyio
    async def test_async_wait_thread(self):
        """async_wait() on a threaded async function."""
        f = invoke_in_thread()(async_sleep_n_add)
        result = f(0.05, 1, 2)
        val = await result.async_wait()
        assert val == 3

    @pytest.mark.anyio
    async def test_await_protocol_thread(self):
        """await task on a threaded async function."""
        f = invoke_in_thread()(async_sleep_n_add)
        result = f(0.05, 3, 4)
        val = await result
        assert val == 7

    @pytest.mark.anyio
    async def test_async_wait_subprocess(self):
        """async_wait() on a subprocess async function."""
        f = invoke_in_sp()(async_sleep_n_add)
        result = f(0.05, 5, 6)
        val = await result.async_wait()
        assert val == 11

    @pytest.mark.anyio
    async def test_async_wait_deferred(self):
        """async_wait() on a deferred async function (local fallback)."""
        f = deferred_call(allow_local=True)(async_sleep_n_add)
        result = f(0.05, 7, 8)
        val = await result.async_wait()
        assert val == 15

    @pytest.mark.anyio
    async def test_concurrent_gather(self):
        """asyncio.gather on several async-function tasks works."""
        f = invoke_in_thread()(async_sleep_n_add)
        t1 = f(0.2, 1, 2)
        t2 = f(0.2, 3, 4)
        t3 = f(0.2, 5, 6)

        import time
        start = time.monotonic()
        r1, r2, r3 = await asyncio.gather(t1, t2, t3)
        elapsed = time.monotonic() - start

        assert r1 == 3
        assert r2 == 7
        assert r3 == 11
        # should take ~0.2s, not ~0.6s
        assert elapsed < 0.4, f"took {elapsed:.2f}s, expected <0.4s"


# ---------------------------------------------------------------------------
# LMTTask wraps correctly for async functions
# ---------------------------------------------------------------------------

class TestLMTTaskAsyncFunction:
    """LMTTask correctly delegates for async-function tasks."""

    def test_task_attribute_thread(self):
        """LMTTask._task is a ThreadedCall for async functions with invoke_in_thread."""
        f = invoke_in_thread()(async_sleep_n_add)
        result = f(0.05, 1, 2)
        assert isinstance(result, LMTTask)
        assert isinstance(result._task, ThreadedCall)

    def test_task_attribute_subprocess(self):
        """LMTTask._task is a MultiProcessedCall for async functions with invoke_in_sp."""
        f = invoke_in_sp()(async_sleep_n_add)
        result = f(0.05, 1, 2)
        assert isinstance(result, LMTTask)
        assert isinstance(result._task, (ThreadedCall, object))  # MultiProcessedCall

    def test_task_attribute_deferred(self):
        """LMTTask._task is an ExternallyDeferredCall for async functions with deferred_call."""
        f = deferred_call(allow_local=True)(async_sleep_n_add)
        result = f(0.05, 1, 2)
        assert isinstance(result, LMTTask)
        assert isinstance(result._task, ExternallyDeferredCall)
