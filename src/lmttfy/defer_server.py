"""Task execution server for ``lmttfy`` deferred tasks.

Consumes tasks from a Redis/Valkey queue and executes them locally in a
configurable thread pool.

CLI usage::

    python -m lmttfy.defer_server --redis-url redis://127.0.0.1:6379/0

Programmatic usage::

    from lmttfy.defer_server import run_server
    run_server(redis_url="redis://...", max_threads=10)
"""

import argparse
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from .deferred import (
    _deserialise,
    _import_function,
    _task_hash_key,
    _TASK_QUEUE_KEY,
    _try_connect,
)

logger = logging.getLogger(__name__)

_WORKER_SHUTDOWN = threading.Event()


def run_server(
    redis_url: Optional[str] = None,
    max_threads: int = 10,
    max_threads_per_func: int = 0,
    queue_key: str = _TASK_QUEUE_KEY,
):
    """Start the task execution server.

    Parameters
    ----------
    redis_url:
        Redis/Valkey connection URL.  Falls back to ``LMTTFY_REDIS_URL``
        env var, then ``redis://127.0.0.1:6379/0``.
    max_threads:
        Maximum number of worker threads for the thread pool.
    max_threads_per_func:
        Maximum concurrent executions per unique function (0 = unlimited).
    queue_key:
        Redis list key to consume tasks from.
    """
    client, backend_type = _try_connect(redis_url)
    if client is None:
        logger.error("cannot start server: no Redis/Valkey backend reachable")
        sys.exit(1)

    logger.info(
        "defer server connected to %s, pool=%d workers, queue=%s",
        backend_type, max_threads, queue_key,
    )

    per_func_semaphores: Dict[str, threading.BoundedSemaphore] = {}
    per_func_lock = threading.Lock()

    def _get_sem(func_name: str) -> Optional[threading.BoundedSemaphore]:
        if max_threads_per_func <= 0:
            return None
        with per_func_lock:
            if func_name not in per_func_semaphores:
                per_func_semaphores[func_name] = threading.BoundedSemaphore(
                    max_threads_per_func
                )
            return per_func_semaphores[func_name]

    def _execute(task_id: str, task_data: Dict[bytes, bytes]):
        func_name = task_data.get(b"func_name", b"").decode()
        logger.info("executing task %s: %s", task_id, func_name)

        try:
            func = _import_function(func_name)
            args = _deserialise(task_data.get(b"args_b64", b"").decode())
            kwargs = _deserialise(task_data.get(b"kwargs_b64", b"").decode())

            sem = _get_sem(func_name)
            if sem is not None:
                sem.acquire()

            try:
                result = func(*args, **kwargs)
                encoded = base64_encode(pickle_dumps(result))
                client.hset(
                    _task_hash_key(task_id),
                    mapping={
                        "status": "done",
                        "result_b64": encoded,
                    },
                )
                logger.info("task %s completed successfully", task_id)
            except Exception as exc:
                logger.error("task %s failed: %s", task_id, exc)
                client.hset(
                    _task_hash_key(task_id),
                    mapping={
                        "status": "error",
                        "error": str(exc),
                    },
                )
            finally:
                if sem is not None:
                    sem.release()

        except Exception as setup_err:
            logger.error("failed to set up task %s: %s", task_id, setup_err)
            client.hset(
                _task_hash_key(task_id),
                mapping={
                    "status": "error",
                    "error": str(setup_err),
                },
            )

    def _worker():
        while not _WORKER_SHUTDOWN.is_set():
            try:
                _, raw_id = client.brpop(queue_key, timeout=2)
                if raw_id is None:
                    continue
                task_id = raw_id.decode()
            except Exception:
                continue

            task_data = client.hgetall(_task_hash_key(task_id))
            if not task_data:
                continue

            pool.submit(_execute, task_id, task_data)

    with ThreadPoolExecutor(max_workers=max_threads) as pool:
        workers = [
            threading.Thread(target=_worker, daemon=True)
            for _ in range(max(2, min(max_threads, 4)))
        ]
        for w in workers:
            w.start()

        logger.info("defer server running (pid=%d)", os.getpid())

        # wait for shutdown signal
        _WORKER_SHUTDOWN.wait()

    logger.info("defer server shutting down")


def base64_encode(data: bytes) -> str:
    """Encode bytes as base64 ASCII string."""
    import base64
    return base64.b64encode(data).decode("ascii")


def pickle_dumps(obj: Any) -> bytes:
    """Pickle an object to bytes."""
    import pickle
    return pickle.dumps(obj)


def main():
    parser = argparse.ArgumentParser(description="lmttfy deferred task server")
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("LMTTFY_REDIS_URL", "redis://127.0.0.1:6379/0"),
        help="Redis/Valkey connection URL",
    )
    parser.add_argument(
        "--max-threads",
        type=int,
        default=10,
        help="maximum worker threads (default: 10)",
    )
    parser.add_argument(
        "--max-threads-per-func",
        type=int,
        default=0,
        help="max concurrent executions per function, 0 = unlimited (default: 0)",
    )
    parser.add_argument(
        "--queue-key",
        default=_TASK_QUEUE_KEY,
        help="Redis list key for the task queue (default: lmttfy:queue)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="enable debug logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    def _handle_sig(*_):
        logger.info("shutdown signal received")
        _WORKER_SHUTDOWN.set()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    run_server(
        redis_url=args.redis_url,
        max_threads=args.max_threads,
        max_threads_per_func=args.max_threads_per_func,
        queue_key=args.queue_key,
    )


if __name__ == "__main__":
    main()
