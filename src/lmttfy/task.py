"""Unified task wrapper that provides a consistent API across all executors."""

from typing import Any, Callable


class LMTTask:
    """Unified return type for all lmttfy decorators.

    Wraps any internal task object (``ThreadedCall``, ``MultiProcessedCall``,
    or ``ExternallyDeferredCall``) and exposes the same public API::

        task = some_decorated_func(...)

        # block until done
        result = task.wait()
        result = task.wait(timeout=5.0)

        # raise the captured exception, if any
        task.burst()

        # register callbacks
        task.on_complete(print)
        task.on_error(lambda exc: print("error:", exc))
    """

    def __init__(self, task: Any) -> None:
        #: The wrapped internal task object (public for developer access).
        self._task = task

    def wait(self, timeout: float = 0) -> Any:
        """Wait for the wrapped function to return or error-out.

        Returns the return value, or ``None`` when *timeout* expires before
        the task completes.
        """
        return self._task.wait(timeout=timeout)

    def burst(self) -> None:
        """Raise the captured exception from the task, if any."""
        self._task.burst()

    def on_complete(self, func: Callable, immediate_callback_if_done: bool = True):
        """Register a callback invoked with the return value on success.

        Returns ``self`` for chaining.
        """
        self._task.on_complete(func, immediate_callback_if_done)
        return self

    def on_error(self, func: Callable, immediate_callback_if_done: bool = True):
        """Register a callback invoked with the exception on failure.

        Returns ``self`` for chaining.
        """
        self._task.on_error(func, immediate_callback_if_done)
        return self
