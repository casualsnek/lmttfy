import inspect
from typing import Any, Callable, TypeVar

CALL_STATE_INCOMPLETE: int = 0
CALL_STATE_SUCCESS: int = 1
CALL_STATE_ERROR: int = 2

R = TypeVar('R')  # Maybe @overload ????


def pass_(*args, **kwargs):
    """
    Does nothing at all
    """
    pass


# ---------------------------------------------------------------------------
# callback invocation helpers (sync + async aware)
# ---------------------------------------------------------------------------

def invoke_callback_sync(callback: Callable, arg: Any) -> None:
    """Invoke *callback(arg)* from a **sync** context (worker thread).

    If the callback is an ``async def`` function:
    * When no event loop is running (e.g. worker thread) it is driven to
      completion via ``asyncio.run`` in a new event loop.
    * When an event loop **is** running (e.g. ``on_complete`` called from
      an async context with ``immediate_callback_if_done=True``) the
      coroutine is scheduled via ``asyncio.ensure_future`` so it runs on
      the existing loop instead of failing.

    Plain sync callbacks are called directly.
    """
    if inspect.iscoroutinefunction(callback):
        import asyncio

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # no running loop -- create one
            asyncio.run(callback(arg))
            return
        # running loop -- schedule the coroutine
        asyncio.ensure_future(callback(arg))
    else:
        callback(arg)


async def invoke_callback_async(callback: Callable, arg: Any) -> None:
    """Invoke *callback(arg)* from an **async** context.

    If the callback is an ``async def`` function it is awaited.  Plain sync
    callbacks are called directly (and may block the event loop briefly).
    """
    if inspect.iscoroutinefunction(callback):
        await callback(arg)
    else:
        callback(arg)
