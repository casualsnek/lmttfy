"""Tests for ``async def`` callbacks in ``on_complete`` and ``on_error``."""

from typing import Any

import pytest

from lmttfy.thread import invoke_in_thread
from lmttfy.process import invoke_in_sp
from lmttfy.deferred import deferred_call

from tests.helpers import sleep_n_add, raise_value_error, async_sleep_n_add, async_raise_value_error


# ---------------------------------------------------------------------------
# sync callbacks still work (regression)
# ---------------------------------------------------------------------------

class TestSyncCallbacks:
    """Regular sync callbacks still fire correctly."""

    def test_on_complete_sync_thread(self):
        """on_complete with sync callback works on threaded tasks."""
        f = invoke_in_thread()(sleep_n_add)
        r = f(0.05, 1, 2)
        collected: list[Any] = []
        r.on_complete(collected.append)
        r.wait()
        assert collected == [3]

    def test_on_error_sync_thread(self):
        """on_error with sync callback works on threaded tasks."""
        f = invoke_in_thread()(raise_value_error)
        r = f("sync_err")
        collected: list[Any] = []
        r.on_error(lambda e: collected.append(str(e)))
        r.wait()
        assert len(collected) == 1

    def test_on_complete_sync_subprocess(self):
        """on_complete with sync callback works on subprocess tasks."""
        f = invoke_in_sp()(sleep_n_add)
        r = f(0.05, 1, 2)
        collected: list[Any] = []
        r.on_complete(collected.append)
        r.wait()
        assert collected == [3]

    def test_on_complete_sync_deferred(self):
        """on_complete with sync callback works on deferred (local) tasks."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        r = f(0.05, 1, 2)
        collected: list[Any] = []
        r.on_complete(collected.append)
        r.wait()
        assert collected == [3]


# ---------------------------------------------------------------------------
# async callbacks from sync context (worker threads)
# ---------------------------------------------------------------------------

class TestAsyncCallbacksSyncContext:
    """Async def callbacks driven via asyncio.run() in worker threads."""

    def test_on_complete_async_thread(self):
        """async def on_complete callback fires on threaded task."""
        f = invoke_in_thread()(sleep_n_add)
        r = f(0.05, 10, 20)
        collected: list[Any] = []

        async def cb(val: Any) -> None:
            collected.append(val)

        r.on_complete(cb)
        r.wait()
        assert collected == [30]

    def test_on_error_async_thread(self):
        """async def on_error callback fires on threaded task."""
        f = invoke_in_thread()(raise_value_error)
        r = f("async_cb_err")
        collected: list[Any] = []

        async def cb(exc: Any) -> None:
            collected.append(str(exc))

        r.on_error(cb)
        r.wait()
        assert len(collected) == 1

    def test_on_complete_async_subprocess(self):
        """async def on_complete callback fires on subprocess task."""
        f = invoke_in_sp()(sleep_n_add)
        r = f(0.05, 100, 200)
        collected: list[Any] = []

        async def cb(val: Any) -> None:
            collected.append(val)

        r.on_complete(cb)
        r.wait()
        assert collected == [300]

    def test_on_complete_async_deferred(self):
        """async def on_complete callback fires on deferred (local) task."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        r = f(0.05, 3, 4)
        collected: list[Any] = []

        async def cb(val: Any) -> None:
            collected.append(val)

        r.on_complete(cb)
        r.wait()
        assert collected == [7]


# ---------------------------------------------------------------------------
# async callbacks from async context (async_wait)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
class TestAsyncCallbacksAsyncContext:
    """Async def callbacks are awaited properly when fired from async_wait()."""

    async def test_on_complete_async_deferred_async_wait(self):
        """async def on_complete with async_wait() on deferred task."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        collected: list[Any] = []

        async def cb(val: Any) -> None:
            collected.append(val * 2)  # transform in the callback

        # register callback *before* calling the function so it fires
        # from the worker thread (not immediately in on_complete)
        r = f(0.3, 5, 6)
        r.on_complete(cb, immediate_callback_if_done=False)
        await r.async_wait()
        assert collected == [22]  # 11 * 2

    async def test_on_error_async_deferred_async_wait(self):
        """async def on_error with async_wait() on deferred task."""
        f = deferred_call(allow_local=True)(async_raise_value_error)
        collected: list[Any] = []

        async def cb(exc: Any) -> None:
            collected.append(str(exc))

        r = f("async_ctx_err")
        r.on_error(cb, immediate_callback_if_done=False)
        await r.async_wait()
        assert len(collected) == 1
        assert "async_ctx_err" in str(collected[0])

    async def test_on_complete_async_thread_async_wait(self):
        """async def on_complete with async_wait() on threaded task."""
        f = invoke_in_thread()(sleep_n_add)
        collected: list[Any] = []

        async def cb(val: Any) -> None:
            collected.append(val)

        r = f(0.3, 1, 2)
        r.on_complete(cb, immediate_callback_if_done=False)
        await r.async_wait()
        assert collected == [3]
