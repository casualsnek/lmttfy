"""Redis/Valkey-backed deferred task execution and the ``@deferred_call`` decorator."""

import base64
import itertools
import json
import logging
import os
import pickle
import random
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import Any, Callable, List, Optional

from .common import CALL_STATE_INCOMPLETE, CALL_STATE_SUCCESS, CALL_STATE_ERROR, pass_
from .exceptions import BurstWhileNoTaskErrorsException
from .task import LMTTask

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# strategy
# ---------------------------------------------------------------------------

class DeferStrategy(Enum):
    """Strategy for picking a server when multiple URLs are configured."""

    #: Try URLs in order; return the first one that connects.
    FIRST_AVAILABLE = "first-available"
    #: Distribute tasks across URLs using a round-robin counter.
    ROUND_ROBIN = "round-robin"
    #: Pick one URL at random for each task.
    RANDOM = "random"


# ---------------------------------------------------------------------------
# global config
# ---------------------------------------------------------------------------

@dataclass
class _DeferredConfig:
    """Mutable global configuration for the deferred client.

    Accessed and mutated through :func:`configure_deferred`.
    """

    #: List of Redis/Valkey URLs to try.
    urls: List[str] = field(default_factory=lambda: [
        os.environ.get(
            "LMTTFY_DEFERRED_URLS",
            os.environ.get("LMTTFY_REDIS_URL", "redis://127.0.0.1:6379/0"),
        )
    ])

    #: Connection password (applied to all URLs that do not already carry one).
    password: Optional[str] = field(
        default_factory=lambda: os.environ.get("LMTTFY_DEFERRED_PASSWORD")
    )

    #: Server-selection strategy.
    strategy: DeferStrategy = field(
        default_factory=lambda: DeferStrategy(
            os.environ.get("LMTTFY_DEFERRED_STRATEGY", DeferStrategy.FIRST_AVAILABLE.value)
        )
    )

    #: Round-robin counter.
    _rr_counter: int = 0


# module-level singleton
_CONFIG = _DeferredConfig()
_CONFIG_LOCK = threading.Lock()


def configure_deferred(
    urls: Optional[List[str]] = None,
    password: Optional[str] = None,
    strategy: Optional[DeferStrategy] = None,
) -> None:
    """Configure the global deferred client settings.

    All parameters are optional — only the provided values are updated.

    Parameters
    ----------
    urls:
        One or more Redis/Valkey connection URLs.  The client will try them
        according to *strategy*.
    password:
        Connection password.  Applied to URLs that do not already contain
        credentials.
    strategy:
        Server-selection strategy (see :class:`DeferStrategy`).
    """
    with _CONFIG_LOCK:
        if urls is not None:
            _CONFIG.urls = list(urls)
        if password is not None:
            _CONFIG.password = password
        if strategy is not None:
            _CONFIG.strategy = strategy


def _inject_password(url: str, password: Optional[str]) -> str:
    """Insert *password* into *url* if the URL has no ``user:password@`` section."""
    if password is None:
        return url
    if "://" not in url:
        return url
    scheme, _, rest = url.partition("://")
    if "@" in rest:
        return url  # already has credentials
    return f"{scheme}://:{password}@{rest}"


def _pick_url() -> Optional[str]:
    """Return the next URL to try according to the configured strategy.

    Returns ``None`` when the URL list is empty.
    """
    with _CONFIG_LOCK:
        urls = _CONFIG.urls
        if not urls:
            return None
        strategy = _CONFIG.strategy
        if strategy == DeferStrategy.ROUND_ROBIN:
            idx = _CONFIG._rr_counter % len(urls)
            _CONFIG._rr_counter += 1
            return urls[idx]
        elif strategy == DeferStrategy.RANDOM:
            return random.choice(urls)
        else:
            # first-available
            return urls[0]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_TASK_QUEUE_KEY = "lmttfy:queue"


def _task_hash_key(task_id: str) -> str:
    return f"lmttfy:task:{task_id}"


def _qualname(function: Callable) -> str:
    """Return a fully-qualified name for *function* (module:qualname)."""
    return f"{function.__module__}:{function.__qualname__}"


def _import_function(qualname: str) -> Callable:
    """Import and return a function by its fully-qualified name.

    The name must have been produced by :func:`_qualname`.
    """
    import importlib

    mod_name, _, func_name = qualname.rpartition(":")
    mod = importlib.import_module(mod_name)
    return getattr(mod, func_name)


def _try_connect(urls: Optional[List[str]] = None):
    """Try to connect to Redis/Valkey using the provided URLs.

    When *urls* is ``None`` the global config is consulted (including
    strategy).  Returns a ``(client, backend_type)`` tuple or
    ``(None, None)``.
    """
    if urls is None:
        urls = _CONFIG.urls
    if not urls:
        return None, None

    password = _CONFIG.password

    for url in urls:
        effective = _inject_password(url, password)
        try:
            import redis as _redis

            client = _redis.Redis.from_url(
                effective, socket_connect_timeout=2, socket_timeout=2
            )
            client.ping()
            return client, "redis"
        except Exception:
            pass
        try:
            import valkey as _valkey

            client = _valkey.Valkey.from_url(
                effective, socket_connect_timeout=2, socket_timeout=2
            )
            client.ping()
            return client, "valkey"
        except Exception:
            pass
    return None, None


def _try_connect_one(redis_url: Optional[str] = None):
    """Convenience: try a single URL, falling back to the global config.

    Used by :class:`ExternallyDeferredCall` when an explicit URL was passed
    to the decorator.
    """
    if redis_url is not None:
        return _try_connect([redis_url])
    # strategy-aware: try one URL at a time using the configured strategy
    url = _pick_url()
    if url is None:
        return None, None
    return _try_connect([url])


# ---------------------------------------------------------------------------
# serialisation helpers
# ---------------------------------------------------------------------------

def _serialise(obj: Any) -> str:
    return base64.b64encode(pickle.dumps(obj)).decode("ascii")


def _deserialise(raw: str) -> Any:
    return pickle.loads(base64.b64decode(raw))


# ---------------------------------------------------------------------------
# ExternallyDeferredCall
# ---------------------------------------------------------------------------

class ExternallyDeferredCall:
    """Represents a task that may execute remotely (Redis/Valkey) or locally.

    When a backend is reachable the task is pushed to a Redis queue and a
    remote worker picks it up.  When **no** backend is reachable and
    ``allow_local`` is ``True`` the task falls back to an in-process thread.
    """

    def __init__(
        self,
        function: Callable,
        args: tuple,
        kwargs: dict,
        *,
        allow_local: bool = True,
        redis_url: Optional[str] = None,
    ) -> None:
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._allow_local = allow_local
        self._redis_url = redis_url

        self._task_id: Optional[str] = None
        self._client: Any = None
        self._backend_type: Optional[str] = None
        self._result: Any = None
        self._exception: Exception = BurstWhileNoTaskErrorsException(
            "burst called with no error on deferred task"
        )
        self._state: int = CALL_STATE_INCOMPLETE
        self._on_complete: Callable = pass_
        self._on_error: Callable = pass_
        self._local_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        self._submit()

    # -- public API -------------------------------------------------------

    def wait(self, timeout: float = 0) -> Any:
        if self._local_thread is not None:
            if timeout > 0:
                self._local_thread.join(timeout=timeout)
            else:
                self._local_thread.join()
            return self._result
        return self._poll_result(timeout=timeout)

    async def async_wait(self, timeout: float = 0) -> Any:
        """Wait asynchronously without blocking the event loop."""
        import asyncio

        if self._local_thread is not None:
            # local fallback: poll state with asyncio.sleep
            deadline = None
            if timeout > 0:
                deadline = time.monotonic() + timeout
            while True:
                with self._lock:
                    if self._state != CALL_STATE_INCOMPLETE:
                        return self._result
                if deadline is not None and time.monotonic() >= deadline:
                    return None
                await asyncio.sleep(0.05)
        else:
            # remote: async poll with run_in_executor for redis calls
            return await self._async_poll_result(timeout=timeout)

    def burst(self) -> None:
        raise self._exception

    def on_complete(self, func: Callable, immediate_callback_if_done: bool = True):
        self._on_complete = func
        if self._state == CALL_STATE_SUCCESS and immediate_callback_if_done:
            func(self._result)
        return self

    def on_error(self, func: Callable, immediate_callback_if_done: bool = True):
        self._on_error = func
        if self._state == CALL_STATE_ERROR and immediate_callback_if_done:
            func(self._exception)
        return self

    # -- internals --------------------------------------------------------

    def _submit(self):
        client, backend = _try_connect_one(self._redis_url)
        if client is not None:
            self._client = client
            self._backend_type = backend
            self._task_id = str(uuid.uuid4())
            payload = {
                "func_name": _qualname(self._function),
                "args_b64": _serialise(self._args),
                "kwargs_b64": _serialise(self._kwargs),
                "status": "pending",
            }
            pipe = self._client.pipeline()
            pipe.hset(_task_hash_key(self._task_id), mapping=payload)
            pipe.lpush(_TASK_QUEUE_KEY, self._task_id)
            pipe.execute()
            logger.info(
                "deferred task %s pushed to %s queue", self._task_id, self._backend_type
            )
        elif self._allow_local:
            logger.info(
                "no backend available, executing %s locally", _qualname(self._function)
            )
            self._exec_local()
        else:
            msg = (
                f"no backend available and allow_local=False "
                f"for {_qualname(self._function)}"
            )
            raise RuntimeError(msg)

    def _exec_local(self):
        """Run the function in a background thread and store state locally."""
        def _run():
            try:
                import asyncio, inspect
                if inspect.iscoroutinefunction(self._function):
                    ret = asyncio.run(self._function(*self._args, **self._kwargs))
                else:
                    ret = self._function(*self._args, **self._kwargs)
                with self._lock:
                    self._result = ret
                    self._state = CALL_STATE_SUCCESS
                    self._on_complete(ret)
            except Exception as exc:
                with self._lock:
                    self._exception = exc
                    self._state = CALL_STATE_ERROR
                    self._on_error(exc)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        self._local_thread = t

    def _poll_result(self, timeout: float = 0) -> Any:
        """Poll Redis until the task completes or *timeout* expires."""
        deadline = None
        if timeout > 0:
            deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                if self._state == CALL_STATE_SUCCESS:
                    return self._result
                if self._state == CALL_STATE_ERROR:
                    raise self._exception

            if self._client is not None:
                tid = self._task_id
                assert tid is not None
                data = self._client.hgetall(_task_hash_key(tid))
                if data:
                    status = data.get(b"status", b"").decode()
                    if status == "done":
                        raw = data.get(b"result_b64", b"")
                        with self._lock:
                            self._result = _deserialise(raw.decode())
                            self._state = CALL_STATE_SUCCESS
                            self._on_complete(self._result)
                        return self._result
                    elif status == "error":
                        err_msg = data.get(b"error", b"unknown error").decode()
                        exc = RuntimeError(err_msg)
                        with self._lock:
                            self._exception = exc
                            self._state = CALL_STATE_ERROR
                            self._on_error(exc)
                        raise exc

            if deadline is not None and time.monotonic() >= deadline:
                return None
            time.sleep(0.1)

    async def _async_poll_result(self, timeout: float = 0) -> Any:
        """Async variant of _poll_result — uses asyncio.sleep and
        run_in_executor for redis calls so the event loop is not blocked."""
        import asyncio

        loop = asyncio.get_running_loop()
        deadline = None
        if timeout > 0:
            deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                if self._state == CALL_STATE_SUCCESS:
                    return self._result
                if self._state == CALL_STATE_ERROR:
                    raise self._exception

            if self._client is not None:
                tid = self._task_id
                assert tid is not None
                data = await loop.run_in_executor(
                    None, self._client.hgetall, _task_hash_key(tid)
                )
                if data:
                    status = data.get(b"status", b"").decode()
                    if status == "done":
                        raw = data.get(b"result_b64", b"")
                        with self._lock:
                            self._result = _deserialise(raw.decode())
                            self._state = CALL_STATE_SUCCESS
                            self._on_complete(self._result)
                        return self._result
                    elif status == "error":
                        err_msg = data.get(b"error", b"unknown error").decode()
                        exc = RuntimeError(err_msg)
                        with self._lock:
                            self._exception = exc
                            self._state = CALL_STATE_ERROR
                            self._on_error(exc)
                        raise exc

            if deadline is not None and time.monotonic() >= deadline:
                return None
            await asyncio.sleep(0.1)


# ---------------------------------------------------------------------------
# decorator
# ---------------------------------------------------------------------------

def deferred_call(
    allow_local: bool = True,
    redis_url: Optional[str] = None,
):
    """Decorator that dispatches function calls through a Redis/Valkey queue.

    When a queue backend is reachable the call is pushed as a remote task
    and an :class:`LMTTask` wrapping an :class:`ExternallyDeferredCall` is
    returned immediately.

    When **no** backend is reachable and ``allow_local=True`` (the default)
    the function runs in-process in a background thread and still returns a
    valid :class:`LMTTask`.

    Parameters
    ----------
    allow_local:
        Fall back to in-process threaded execution when Redis/Valkey is
        unavailable.
    redis_url:
        Redis/Valkey connection URL.  Overrides the global config (see
        :func:`configure_deferred` and the ``LMTTFY_DEFERRED_URLS`` / ``LMTTFY_REDIS_URL``
        environment variables).  Default ``redis://127.0.0.1:6379/0``.

    Usage::

        from lmttfy.deferred import deferred_call

        @deferred_call()
        def process_order(order_id: str) -> dict:
            ...
            return {"status": "ok"}

        task = process_order("ord-42")
        result = task.wait()
    """

    def decorator(function: Callable) -> Callable[..., LMTTask]:
        @wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> LMTTask:
            return LMTTask(
                ExternallyDeferredCall(
                    function, args, kwargs,
                    allow_local=allow_local,
                    redis_url=redis_url,
                )
            )

        return wrapper

    return decorator
