"""Tests for :mod:`lmttfy.thread` — :class:`ThreadedCall`."""

from typing import Any

import pytest

from lmttfy.thread import invoke_in_thread
from lmttfy.exceptions import MaxConcurrentCallsLimitExceedException, BurstWhileNoTaskErrorsException

from tests.helpers import sleep_n_add, raise_value_error


# ---------------------------------------------------------------------------
# Basic smoke tests
# ---------------------------------------------------------------------------

class TestInvokeInThread:
    """Basic sanity checks for the threaded decorator."""

    def test_single_call(self):
        """A single decorated call returns the correct value."""
        f = invoke_in_thread()(sleep_n_add)
        result = f(0.1, 1, 2)
        assert result.wait() == 3

    def test_multiple_concurrent_calls(self):
        """Multiple concurrent calls all resolve correctly."""
        f = invoke_in_thread()(sleep_n_add)
        calls = [f(0.3, i, i * 10) for i in range(5)]
        for i, c in enumerate(calls):
            assert c.wait() == i + i * 10, f"call {i} mismatch"

    def test_wait_with_timeout(self):
        """wait(timeout=…) returns the correct value when the thread completes within the timeout."""
        f = invoke_in_thread()(sleep_n_add)
        result = f(0.05, 42, 1)
        assert result.wait(timeout=5) == 43

    def test_wait_timeout_expires(self):
        """wait(timeout=…) returns None when the thread hasn't completed yet — the value is
        available on a second wait."""
        f = invoke_in_thread()(sleep_n_add)
        result = f(2, 1, 2)
        assert result.wait(timeout=0.01) is None  # still incomplete
        # A subsequent wait without timeout gets the value
        assert result.wait() == 3


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestThreadedCallErrors:
    """Error propagation and :meth:`~ThreadedCall.burst` behaviour."""

    def test_burst_raises_captured_exception(self):
        """burst() raises the exception that occurred inside the threaded function."""
        f = invoke_in_thread()(raise_value_error)
        result = f("oops")
        result.wait()  # let it finish
        with pytest.raises(ValueError, match="oops"):
            result.burst()

    def test_burst_before_any_error(self):
        """burst() raises BurstWhileNoTaskErrorsException if no error has been captured."""
        f = invoke_in_thread()(sleep_n_add)
        result = f(0.1, 1, 2)
        result.wait()
        with pytest.raises(BurstWhileNoTaskErrorsException):
            result.burst()

    def test_on_complete_callback(self):
        """The on_complete callback receives the return value."""
        f = invoke_in_thread()(sleep_n_add)
        result = f(0.1, 1, 2)
        collected: list[Any] = []
        result.on_complete(collected.append)
        result.wait()
        assert collected == [3]

    def test_on_error_callback(self):
        """The on_error callback fires when the function raises."""
        f = invoke_in_thread()(raise_value_error)
        result = f("callback_err")
        collected: list[Any] = []
        result.on_error(lambda e: collected.append(e))
        result.wait()
        assert len(collected) == 1
        assert isinstance(collected[0], ValueError)
        assert str(collected[0]) == "callback_err"


# ---------------------------------------------------------------------------
# Concurrency limiting
# ---------------------------------------------------------------------------

class TestThreadedCallConcurrency:
    """Max-concurrent-calls guard."""

    def test_max_concurrent_calls_exceeded(self):
        """Calling beyond *max_concurrent_execs* raises immediately."""
        f = invoke_in_thread(max_concurrent_execs=2)(sleep_n_add)

        # Start two calls (they stay in-flight for 1 s each)
        c1 = f(1, 0, 0)
        c2 = f(1, 0, 0)

        # Third call should be rejected
        with pytest.raises(MaxConcurrentCallsLimitExceedException):
            f(1, 0, 0)

        # Let the first two finish
        c1.wait()
        c2.wait()

    def test_max_concurrent_recovery(self):
        """After a call completes, the counter frees up so a new call can be made."""
        f = invoke_in_thread(max_concurrent_execs=1)(sleep_n_add)

        c1 = f(0.1, 1, 2)
        c1.wait()

        # Now a new call should succeed
        c2 = f(0.1, 3, 4)
        assert c2.wait() == 7
