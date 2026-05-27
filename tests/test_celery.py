"""Tests for Celery integration with ``@deferred_call``.

These tests verify that:
- A ``celery.Celery`` app can be wired via :func:`configure_deferred`
- Functions are auto-registered as Celery tasks
- When the Celery broker is unreachable, the task falls back to local execution
  (or raises if ``allow_local=False``)
"""

from typing import Any

import pytest
from celery import Celery

from lmttfy.deferred import deferred_call, configure_deferred, _CONFIG
from lmttfy.task import LMTTask
from lmttfy.exceptions import BurstWhileNoTaskErrorsException

from tests.helpers import sleep_n_add, raise_value_error


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_config():
    """Reset the global deferred config before each test."""
    prev_celery_app = _CONFIG.celery_app
    yield
    _CONFIG.celery_app = prev_celery_app


def _make_app():
    """Create a throwaway Celery app with an in-memory broker for testing."""
    return Celery("lmttfy_test", broker="memory://", task_always_eager=False)


# ---------------------------------------------------------------------------
# configure_deferred with celery
# ---------------------------------------------------------------------------

class TestCeleryConfig:
    """Configuring a Celery app via configure_deferred."""

    def test_configure_celery_app(self):
        """A Celery app can be set via configure_deferred."""
        app = _make_app()
        configure_deferred(celery_app=app)
        assert _CONFIG.celery_app is app

    def test_configure_updates_only_celery(self):
        """Partial configure_deferred only touches celery_app."""
        app = _make_app()
        configure_deferred(celery_app=app)
        assert _CONFIG.celery_app is app


# ---------------------------------------------------------------------------
# auto-registration
# ---------------------------------------------------------------------------

class TestCeleryAutoRegister:
    """@deferred_call auto-registers with the configured Celery app."""

    def test_task_is_registered(self):
        """The function is registered as a Celery task."""
        app = _make_app()
        configure_deferred(celery_app=app)

        @deferred_call()
        def my_task(x: int, y: int) -> int:
            return x + y

        # should be registered in the Celery app's registry
        task_name = f"{my_task.__module__}:{my_task.__qualname__}"
        assert task_name in app.tasks

    def test_registered_task_delays(self):
        """The Celery task can be .delay()ed via the registered name."""
        app = _make_app()
        configure_deferred(celery_app=app)

        @deferred_call()
        def add(x: int, y: int) -> int:
            return x + y

        task_name = f"tests.test_celery:{add.__qualname__}"
        registered = app.tasks[task_name]
        # delay() sends to broker — with memory:// it stays in-process
        async_result = registered.delay(1, 2)
        # "memory://" with eager=False means it won't actually run without a worker
        assert async_result.id is not None


# ---------------------------------------------------------------------------
# local fallback when celery is configured
# ---------------------------------------------------------------------------

class TestCeleryLocalFallback:
    """When the Celery broker is unreachable, local fallback kicks in."""

    def test_local_fallback_with_celery_configured(self):
        """allow_local=True falls back to local execution when broker is down."""
        # use a non-existent broker URL
        app = Celery("test", broker="redis://127.0.0.1:1/0")
        configure_deferred(celery_app=app)

        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 1, 2)
        assert result.wait() == 3

    def test_no_fallback_when_disallowed(self):
        """allow_local=False raises when the Celery broker is unreachable."""
        app = Celery("test", broker="redis://127.0.0.1:1/0")
        configure_deferred(celery_app=app)

        f = deferred_call(allow_local=False)(sleep_n_add)
        with pytest.raises(RuntimeError, match="no backend"):
            f(0.05, 1, 2)

    def test_error_captured_in_local_fallback(self):
        """Errors in local fallback with celery configured are captured."""
        app = Celery("test", broker="redis://127.0.0.1:1/0")
        configure_deferred(celery_app=app)

        f = deferred_call(allow_local=True)(raise_value_error)
        result = f("celery_local_err")
        result.wait()
        with pytest.raises(ValueError, match="celery_local_err"):
            result.burst()

    def test_on_complete_local_fallback(self):
        """on_complete fires in local fallback with celery configured."""
        app = Celery("test", broker="redis://127.0.0.1:1/0")
        configure_deferred(celery_app=app)

        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 10, 20)
        collected: list[Any] = []
        result.on_complete(collected.append)
        result.wait()
        assert collected == [30]

    def test_on_error_local_fallback(self):
        """on_error fires in local fallback with celery configured."""
        app = Celery("test", broker="redis://127.0.0.1:1/0")
        configure_deferred(celery_app=app)

        f = deferred_call(allow_local=True)(raise_value_error)
        result = f("celery_on_error")
        collected: list[Any] = []
        result.on_error(lambda e: collected.append(e))
        result.wait()
        assert len(collected) == 1

    def test_chaining(self):
        """on_complete/on_error return self with celery configured."""
        app = Celery("test", broker="redis://127.0.0.1:1/0")
        configure_deferred(celery_app=app)

        f = deferred_call(allow_local=True)(sleep_n_add)
        result = f(0.05, 1, 2)
        assert result.on_complete(print) is result
        assert result.on_error(print) is result
