"""Tests for :mod:`lmttfy.deferred` — :class:`ExternallyDeferredCall` and :func:`deferred_call`."""

from typing import Any

import pytest

from lmttfy.deferred import deferred_call, ExternallyDeferredCall
from lmttfy.task import LMTTask
from lmttfy.exceptions import BurstWhileNoTaskErrorsException

from tests.helpers import sleep_n_add, raise_value_error


# ---------------------------------------------------------------------------
# local-fallback smoke tests  (no Redis/Valkey needed)
# ---------------------------------------------------------------------------

class TestDeferredCallLocalFallback:
    """When no backend is reachable and allow_local=True the function runs in-process."""

    def test_local_fallback_basic(self):
        """Single deferred call returns the correct value via local fallback."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 1, 2)
        assert isinstance(result, LMTTask)
        assert isinstance(result._task, ExternallyDeferredCall)
        assert result.wait() == 3

    def test_local_fallback_multi_concurrent(self):
        """Multiple concurrent deferred calls all resolve with local fallback."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        calls = [f(0.1, i, i * 10) for i in range(5)]
        for i, c in enumerate(calls):
            assert c.wait() == i + i * 10, f"call {i} mismatch"

    def test_local_fallback_wait_timeout(self):
        """wait(timeout=…) works with local fallback."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 42, 1)
        assert result.wait(timeout=5) == 43

    def test_local_fallback_wait_timeout_expires(self):
        """wait(timeout=…) returns None when timeout expires during local fallback."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(2, 1, 2)
        assert result.wait(timeout=0.01) is None  # still running
        assert result.wait() == 3  # now complete


# ---------------------------------------------------------------------------
# error handling — local fallback
# ---------------------------------------------------------------------------

class TestDeferredCallErrors:
    """Error propagation for the local-fallback path."""

    def test_burst_raises_captured_exception(self):
        """burst() raises the exception that occurred inside the deferred function."""
        f = deferred_call(allow_local=True)(raise_value_error)
        result = f("oops")
        result.wait()
        with pytest.raises(ValueError, match="oops"):
            result.burst()

    def test_burst_before_any_error(self):
        """burst() raises BurstWhileNoTaskErrorsException if no error captured."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 1, 2)
        result.wait()
        with pytest.raises(BurstWhileNoTaskErrorsException):
            result.burst()

    def test_on_complete_callback(self):
        """on_complete callback fires with the return value."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 1, 2)
        collected: list[Any] = []
        result.on_complete(collected.append)
        result.wait()
        assert collected == [3]

    def test_on_error_callback(self):
        """on_error callback fires when the function raises."""
        f = deferred_call(allow_local=True)(raise_value_error)
        result = f("callback_err")
        collected: list[Any] = []
        result.on_error(lambda e: collected.append(e))
        result.wait()
        assert len(collected) == 1
        assert isinstance(collected[0], ValueError)
        assert str(collected[0]) == "callback_err"

    def test_chaining(self):
        """on_complete() on_error() return self for chaining."""
        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 1, 2)
        assert result.on_complete(print) is result
        assert result.on_error(print) is result


# ---------------------------------------------------------------------------
# allow_local=False
# ---------------------------------------------------------------------------

class TestDeferredCallNoFallback:
    """When allow_local=False and no backend is reachable, the decorator raises."""

    def test_raises_on_first_call(self):
        """Calling a deferred function with allow_local=False raises RuntimeError."""
        f = deferred_call(allow_local=False)(sleep_n_add)
        with pytest.raises(RuntimeError, match="no backend"):
            f(0.05, 1, 2)

    def test_raises_on_error_function(self):
        """Same behaviour for error-raising functions."""
        f = deferred_call(allow_local=False)(raise_value_error)
        with pytest.raises(RuntimeError, match="no backend"):
            f("nope")


# ---------------------------------------------------------------------------
# ExternallyDeferredCall internals (local fallback)
# ---------------------------------------------------------------------------

class TestExternallyDeferredCallDirect:
    """Direct construction of ExternallyDeferredCall with local fallback."""

    def test_direct_construction(self):
        """Manually constructing an ExternallyDeferredCall works."""
        edc = ExternallyDeferredCall(
            sleep_n_add, (0.05, 10, 20), {},
            allow_local=True,
        )
        assert edc.wait() == 30

    def test_direct_with_timeout(self):
        """wait(timeout=…) on a directly-constructed ExternallyDeferredCall."""
        edc = ExternallyDeferredCall(
            sleep_n_add, (2, 10, 20), {},
            allow_local=True,
        )
        assert edc.wait(timeout=0.01) is None
        assert edc.wait() == 30

    def test_direct_error(self):
        """ExternallyDeferredCall captures and re-raises exceptions."""
        edc = ExternallyDeferredCall(
            raise_value_error, ("direct_err",), {},
            allow_local=True,
        )
        edc.wait()
        with pytest.raises(ValueError, match="direct_err"):
            edc.burst()
