"""Unified task wrapper that provides a consistent API across all executors."""

from typing import Any, Callable, Optional


class LMTTask:
    """Unified return type for all lmttfy decorators.

    Wraps any internal task object (``ThreadedCall``, ``MultiProcessedCall``,
    or ``ExternallyDeferredCall``) and exposes the same public API::

        task = some_decorated_func(...)

        # block until done
        result = task.wait()
        result = task.wait(timeout=5.0)

        # async: await without blocking the event loop
        result = await task.async_wait()
        result = await task              # same as async_wait()

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
        """Wait (synchronously) for the wrapped function to return or error-out.

        Returns the return value, or ``None`` when *timeout* expires before
        the task completes.

        In async contexts prefer :meth:`async_wait` or ``await task`` to avoid
        blocking the event loop.
        """
        return self._task.wait(timeout=timeout)

    async def async_wait(self, timeout: float = 0) -> Optional[Any]:
        """Wait (asynchronously) for the wrapped function to return or error-out.

        Uses ``asyncio.sleep`` internally so it does **not** block the event
        loop.  Can be used safely inside FastAPI endpoints and other async
        frameworks.

        Returns the return value, or ``None`` when *timeout* expires before
        the task completes.
        """
        return await self._task.async_wait(timeout=timeout)

    def __await__(self):
        """Allow ``await task`` as a shorthand for ``await task.async_wait()``."""
        return self.async_wait().__await__()

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
