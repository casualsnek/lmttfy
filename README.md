# lmttfy

Python module that provides decorators to make a function execute in a different
thread, subprocess, or remote task queue.

## Installation

```bash
pip install lmttfy
```

Requires Python 3.10 or later.

## Overview

lmttfy offers three execution backends, each exposed through a decorator that
preserves the original function signature:

| Decorator | Backend | Use case |
|-----------|---------|----------|
| `@invoke_in_thread()` | Thread pool (in-process) | Lightweight concurrency for I/O-bound work |
| `@invoke_in_sp()` | Subprocess (multiprocessing) | CPU-bound work, GIL-free parallelism |
| `@deferred_call()` | Redis / Valkey queue | Distributed tasks, async offloading |

All three return an `LMTTask` instance with an identical API (`wait`,
`async_wait`, `burst`, `on_complete`, `on_error`), so code can switch between
backends without changing call-site logic.

## Quick start

### Synchronous functions

```python
from lmttfy import invoke_in_thread, invoke_in_sp, deferred_call
from time import sleep


@invoke_in_thread()
def fetch_url(url: str) -> bytes:
    ...


@invoke_in_sp()
def compute_hash(data: bytes) -> str:
    ...


@deferred_call()          # falls back to local thread when Redis is unavailable
def process_order(order_id: str) -> dict:
    ...
```

All three are called identically:

```python
task_a = fetch_url("https://example.com")
task_b = compute_hash(b"hello")
task_c = process_order("ord-42")

result_a = task_a.wait()
result_b = task_b.wait()
result_c = task_c.wait()
```

### Async functions

The same decorators also work with ``async def`` functions.  The decorator
creates a new event loop in the worker thread (or subprocess) and runs the
coroutine to completion::

```python
import httpx


@invoke_in_thread()
async def fetch_json(url: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        return resp.json()


# From a sync context -- wait() blocks the *calling* thread only:
result = fetch_json("https://api.example.com/data").wait()
```

From an async context, use ``async_wait()`` or ``await task`` so the event
loop stays responsive::

```python
import asyncio


async def main():
    t1 = fetch_json("https://api.example.com/a")
    t2 = fetch_json("https://api.example.com/b")

    # await tasks concurrently -- the worker threads do the work
    r1, r2 = await asyncio.gather(t1, t2)
    print(r1, r2)


asyncio.run(main())
```

This pattern works with all three decorators (``@invoke_in_thread``,
``@invoke_in_sp``, ``@deferred_call``).

## LMTTask API

Every decorator returns an `LMTTask` object. The underlying task object is
accessible via `._task` if needed.

```python
task = some_decorated_func(arg1, arg2)
```

### wait(timeout=0)

Blocks until the wrapped function returns.  Returns the return value, or
`None` when *timeout* expires before the task completes.  A *timeout* of
`0` (the default) means wait indefinitely.

```python
result = task.wait()            # block until done
result = task.wait(timeout=5.0) # wait at most 5 seconds
```

### async_wait(timeout=0)

Coroutine that waits without blocking the event loop.  Uses `asyncio.sleep`
internally for polling, so the event loop stays responsive.  Can be called
from FastAPI endpoints, async views, or any `async def` context.

```python
result = await task.async_wait()
result = await task.async_wait(timeout=5.0)
```

The `__await__` protocol is supported so you can also use `await task`
directly:

```python
result = await task    # equivalent to await task.async_wait()
```

Multiple tasks can be gathered concurrently without blocking:

```python
import asyncio

t1, t2, t3 = fetch_url("a"), fetch_url("b"), fetch_url("c")
results = await asyncio.gather(t1, t2, t3)
```

### burst()

If the wrapped function raised an exception, `burst()` re-raises it in the
calling context.  Calling `burst()` on a successful task raises
`BurstWhileNoTaskErrorsException`.

```python
try:
    task.burst()
except ValueError as e:
    print("task failed:", e)
```

### on_complete(callback, immediate_callback_if_done=True)

Registers a callback invoked with the return value when the task succeeds.
Returns `self` for chaining.

```python
task.on_complete(lambda result: print("done:", result))
```

### on_error(callback, immediate_callback_if_done=True)

Registers a callback invoked with the exception when the task fails.
Returns `self` for chaining.

```python
task.on_error(lambda exc: print("error:", exc))
```

## Threaded execution

```python
from lmttfy import invoke_in_thread


@invoke_in_thread(max_concurrent_execs=4)
def download(url: str) -> bytes:
    ...
```

`invoke_in_thread` runs the function in a `threading.Thread`.  The
`max_concurrent_execs` parameter limits how many concurrent calls to the
same function are allowed (`-1` means unlimited).

## Subprocess execution

```python
from lmttfy import invoke_in_sp


@invoke_in_sp(max_concurrent_execs=2, terminate_on_return=True)
def render_frame(scene: str) -> bytes:
    ...
```

`invoke_in_sp` runs the function in a `multiprocessing.Process`.  The
`terminate_on_return` flag kills the subprocess immediately after the
function returns (useful when the process holds resources like loaded
models).  The `max_concurrent_execs` parameter limits concurrent
subprocesses per function.

Note: the decorated function must be importable (not defined inside
`__main__`) -- this is a standard requirement for `multiprocessing`,
especially on Windows where the `spawn` start method is used.

## Deferred (remote) execution

### Decorator

```python
from lmttfy import deferred_call


@deferred_call(allow_local=True, redis_url="redis://localhost:6379/0")
def send_email(to: str, subject: str, body: str) -> dict:
    ...
```

When called, the decorator attempts to connect to a Redis (or Valkey) backend:
- If the backend is reachable: the call is serialised and pushed to a queue
  (`lmttfy:queue`).  An `LMTTask` is returned immediately; the caller can
  `wait()` or `async_wait()` for the result (which polls Redis).
- If the backend is **not** reachable and `allow_local=True` (the default):
  the function runs in a background thread in the current process.
- If the backend is **not** reachable and `allow_local=False`:
  `RuntimeError` is raised immediately.

### Client configuration

Global configuration for the deferred client is managed through
`configure_deferred()`:

```python
from lmttfy.deferred import configure_deferred, DeferStrategy

configure_deferred(
    urls=[
        "redis://server1:6379/0",
        "redis://server2:6379/0",
        "redis://server3:6379/0",
    ],
    password="PLACEHOLDER_REDIS_PASSWORD",
    strategy=DeferStrategy.ROUND_ROBIN,
)
```

Parameters:

- **urls** -- One or more Redis/Valkey URLs.  When the decorator is called
  without an explicit `redis_url`, the client tries these URLs using the
  configured strategy.
- **password** -- Connection password (applied to URLs that do not already
  carry credentials).
- **strategy** -- Server-selection strategy for distributing tasks across
  multiple URLs (see below).

The same settings can be provided through environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LMTTFY_DEFERRED_URLS` | `redis://127.0.0.1:6379/0` | Comma-separated list of URLs |
| `LMTTFY_DEFERRED_PASSWORD` | (none) | Connection password |
| `LMTTFY_DEFERRED_STRATEGY` | `first-available` | `first-available`, `round-robin`, or `random` |

### Server-selection strategies

| Strategy | Behaviour |
|----------|-----------|
| `DeferStrategy.FIRST_AVAILABLE` | Always tries the first URL in the list |
| `DeferStrategy.ROUND_ROBIN` | Cycles through URLs in order across calls |
| `DeferStrategy.RANDOM` | Picks a random URL for each call |

### Task server

A standalone worker process must be running to consume tasks from the queue
when the backend is used in remote mode.

```bash
# install the package, then:
lmttfy-defer-server --redis-url redis://localhost:6379/0 --max-threads 10
```

Or programmatically:

```python
from lmttfy.defer_server import run_server

run_server(
    redis_url="redis://localhost:6379/0",
    max_threads=10,
    max_threads_per_func=2,
)
```

The server:
- BLPOPs task IDs from `lmttfy:queue`
- Executes them in a `ThreadPoolExecutor`
- Stores results back in Redis under `lmttfy:task:<task_id>`
- Optionally limits concurrent executions per function name via
  `max_threads_per_func`
- Handles SIGINT / SIGTERM for graceful shutdown

## Windows compatibility

On Windows, `multiprocessing` uses the `spawn` start method, which requires
the target function to be importable by module path.  Functions decorated
with `@invoke_in_sp()` must be defined in an importable module (not in
`__main__`).

To test Windows-like behaviour on Linux, set the `LMTTFY_TEST_SPAWN`
environment variable:

```bash
LMTTFY_TEST_SPAWN=1 python -m pytest tests/
```

This switches `multiprocessing` to `spawn` mode and propagates the source
directory to child processes via `PYTHONPATH`.

## Exceptions

| Exception | Raised by | When |
|-----------|-----------|------|
| `MaxConcurrentCallsLimitExceedException` | `invoke_in_thread` / `invoke_in_sp` | The per-function concurrency limit is exceeded |
| `BurstWhileNoTaskErrorsException` | `burst()` | `burst()` is called on a successful task |

## Requirements

- Python 3.10+
- `redis` or `valkey` (optional, only needed for the deferred backend)

## License

GPL-3.0
