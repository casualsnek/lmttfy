"""Integration tests for the Redis-backed remote path in ``ExternallyDeferredCall``.

These tests require a running Redis (or Valkey) server on ``127.0.0.1:6379``.
If Redis is not available the tests are skipped with a clear message.

Run with::

    pytest tests/test_deferred_remote.py -v
"""

import os
from typing import Any

import pytest

from lmttfy.deferred import (
    ExternallyDeferredCall,
    deferred_call,
    configure_deferred,
    _try_connect,
    _task_hash_key,
    _TASK_QUEUE_KEY,
)
from lmttfy.task import LMTTask
from lmttfy.exceptions import BurstWhileNoTaskErrorsException

from tests.helpers import sleep_n_add, raise_value_error

# ---------------------------------------------------------------------------
# redis availability check
# ---------------------------------------------------------------------------

_redis_available = None
_redis_client = None


def redis_available():
    """Return True if a Redis/Valkey server is reachable on 127.0.0.1:6379."""
    global _redis_available, _redis_client
    if _redis_available is None:
        client, backend = _try_connect(["redis://127.0.0.1:6379/1"])
        if client is not None:
            _redis_available = True
            _redis_client = client
        else:
            _redis_available = False
    return _redis_available


redis_required = pytest.mark.skipif(
    not redis_available(),
    reason="Redis/Valkey server not available on 127.0.0.1:6379",
)


# ---------------------------------------------------------------------------
# cleanup
# ---------------------------------------------------------------------------

def _cleanup(task_id: Any):
    """Remove task keys from Redis after the test."""
    if _redis_client is not None and task_id is not None:
        try:
            _redis_client.delete(_task_hash_key(task_id))
            _redis_client.lrem(_TASK_QUEUE_KEY, 0, task_id)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

@redis_required
class TestDeferredRemoteBasic:
    """Basic remote execution via Redis."""

    def test_push_and_poll_result(self):
        """A deferred call pushed to Redis returns the correct result via poll."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 10, 20)
        assert isinstance(result, LMTTask)
        assert isinstance(result._task, ExternallyDeferredCall)
        assert result._task._client is not None  # redis was used
        # wait for the result (polls redis)
        val = result.wait()
        assert val == 30
        _cleanup(result._task._task_id)

    def test_wait_timeout_expires(self):
        """wait(timeout=0.01) returns None when the remote task is still pending."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(2, 1, 2)
        # short timeout — task will still be running
        val = result.wait(timeout=0.01)
        assert val is None
        # now wait properly
        val = result.wait()
        assert val == 3
        _cleanup(result._task._task_id)

    def test_async_wait_remote(self):
        """async_wait() works with the remote Redis path."""
        import asyncio

        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 100, 200)
        val = asyncio.run(result.async_wait())
        assert val == 300
        _cleanup(result._task._task_id)


@redis_required
class TestDeferredRemoteErrors:
    """Error handling in the remote path."""

    def test_error_captured_and_bursted(self):
        """A function that raises on the remote worker is captured and burst() re-raises."""
        f = deferred_call(allow_local=True)(raise_value_error)
        result = f("remote_err")
        # the remote worker will store the error — wait reads it
        result.wait()
        with pytest.raises(ValueError, match="remote_err"):
            result.burst()
        _cleanup(result._task._task_id)

    def test_on_complete_callback_remote(self):
        """on_complete fires when the remote task completes."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 1, 2)
        collected: list[Any] = []
        result.on_complete(collected.append)
        result.wait()
        assert collected == [3]
        _cleanup(result._task._task_id)

    def test_on_error_callback_remote(self):
        """on_error fires when the remote task errors."""
        f = deferred_call(allow_local=True)(raise_value_error)
        result = f("remote_cb_err")
        collected: list[Any] = []
        result.on_error(lambda e: collected.append(e))
        result.wait()
        assert len(collected) == 1
        assert isinstance(collected[0], RuntimeError)
        assert "remote_cb_err" in str(collected[0])
        _cleanup(result._task._task_id)


@redis_required
class TestDeferredRemoteManual:
    """Manual construction of ExternallyDeferredCall with remote backend."""

    def test_direct_remote_construction(self):
        """Directly constructing an ExternallyDeferredCall pushes to Redis."""
        edc = ExternallyDeferredCall(
            sleep_n_add, (0.05, 5, 5), {},
            allow_local=True,
        )
        assert edc._client is not None  # remote
        val = edc.wait()
        assert val == 10
        _cleanup(edc._task_id)


@redis_required
class TestRedisQueueIsolation:
    """Ensure task keys in Redis are cleaned up properly."""

    def test_task_data_stored_in_redis(self):
        """Task payload exists in Redis after submission."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 1, 2)
        tid = result._task._task_id
        assert tid is not None
        data = _redis_client.hgetall(_task_hash_key(tid))
        assert data is not None
        assert data.get(b"status") == b"pending"
        # wait for completion
        result.wait()
        data = _redis_client.hgetall(_task_hash_key(tid))
        assert data.get(b"status") == b"done"
        _cleanup(tid)
