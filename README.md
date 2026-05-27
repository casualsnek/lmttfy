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

All three return an `LMTTask` instance with an identical API (`wait`, `burst`,
`on_complete`, `on_error`), so code can switch between backends without
changing call-site logic.

## Quick start

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
`__main__`) — this is a standard requirement for
`multiprocessing`.

## Deferred (remote) execution

### Decorator

```python
from lmttfy import deferred_call


@deferred_call(allow_local=True, redis_url="redis://localhost:6379/0")
def send_email(to: str, subject: str, body: str) -> dict:
    ...
```

When called, the decorator attempts to connect to Redis (or Valkey).
- If the backend is reachable: the call is serialised and pushed to a
  queue (`lmttfy:queue`).  An `LMTTask` is returned immediately; the
  caller can `wait()` for the result (which polls Redis).
- If the backend is *not* reachable and `allow_local=True` (the default):
  the function runs in a background thread in the current process.
- If the backend is *not* reachable and `allow_local=False`:
  `RuntimeError` is raised immediately.

The Redis URL can be set via the `redis_url` parameter or the
`LMTTFY_REDIS_URL` environment variable (default:
`redis://127.0.0.1:6379/0`).

### Task server

A standalone worker process must be running to consume tasks from the
queue when the backend is used in remote mode.

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
