#!/usr/bin/env python3
"""stress_test_redis_py.py -- Integration + stress test via redis-py.

Purpose
    End-to-end smoke test and concurrent stress test for PeaDB (or any
    Redis-compatible server) using the ``redis-py`` client library.  Useful
    for validating basic correctness and estimating throughput after code
    changes.

What it does
    1. Connects to a Redis-compatible server and runs these subtests:
       a. **Basic SET/GET/DEL** -- round-trip correctness.
       b. **Expiry accuracy** -- sets a key with a 2 s TTL, sleeps 3 s,
          verifies the key is gone.
       c. **Atomic INCR** -- 8 threads x 1000 INCR ops; asserts final
          value is 8000.
       d. **Pipeline** -- sends 1000 SET commands in a single pipeline.
       e. **Concurrency stress** -- THREADS threads x OPS_PER_THREAD
          SET+GET ops with data-integrity checks.
       f. **Simple throughput benchmark** -- 20 000 sequential SETs with
          ops/sec output.
    2. Prints per-test progress with checkmarks or raises on failure.

How to run
    1. Start a PeaDB (or Redis) server:

           ./build/peadb-server            # default port 6379

    2. Install the redis-py dependency:

           pip install redis               # or: pip install -r scripts/requirements.txt

    3. Run the script:

           python3 scripts/examples/stress_test_redis_py.py

    No CLI arguments.  To change target host/port or concurrency parameters,
    edit the constants at the top of the file:
        HOST, PORT, THREADS, OPS_PER_THREAD

Prerequisites
    - Python 3.9+
    - ``redis`` package (``pip install redis>=5.0.0``)
    - A running Redis-compatible server on HOST:PORT (default 127.0.0.1:6379)

Interpreting the output
    Success:
        [connected]  Connected to peadb
        [TEST] Basic SET/GET/DEL
          checkmark Basic test passed
        ...
        ALL TESTS PASSED
        (exit code 0)

    Failure:
        TEST FAILED: <error message>
        (exit code 1)

    The throughput numbers (ops/sec) are indicative, not a formal benchmark;
    they depend on client overhead, network, and hardware.

Exit codes
    0   All tests passed.
    1   Any test failed (assertion or exception).

Notes
    - The expiry test sleeps 3 seconds, so total runtime is at least 3 s.
    - THREADS and OPS_PER_THREAD control the stress workload; adjust for
      quicker smoke tests or heavier soak testing.
"""
import random
import string
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import redis

HOST = "127.0.0.1"
PORT = 6379
THREADS = 16
OPS_PER_THREAD = 5000


def random_string(n: int = 16) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def connect():
    r = redis.Redis(host=HOST, port=PORT, decode_responses=True)
    r.ping()
    return r


def test_basic(r):
    print("[TEST] Basic SET/GET/DEL")

    r.set("foo", "bar")
    assert r.get("foo") == "bar"

    r.delete("foo")
    assert r.get("foo") is None

    print("  ✔ Basic test passed")


def test_expiry(r):
    print("[TEST] Expiry accuracy")

    r.set("expire_test", "1", ex=2)
    assert r.get("expire_test") == "1"

    time.sleep(3)
    assert r.get("expire_test") is None

    print("  ✔ Expiry test passed")


def test_atomic_incr(r):
    print("[TEST] Atomic INCR under concurrency")

    r.delete("counter")

    def worker():
        for _ in range(1000):
            r.incr("counter")

    threads = []
    for _ in range(8):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    result = int(r.get("counter"))
    assert result == 8000, f"Expected 8000, got {result}"

    print("  ✔ Atomicity test passed")


def test_pipeline(r):
    print("[TEST] Pipeline performance")

    pipe = r.pipeline()
    for i in range(1000):
        pipe.set(f"pipe:{i}", i)
    pipe.execute()

    print("  ✔ Pipeline test passed")


def stress_worker(thread_id: int) -> None:
    r = connect()
    for i in range(OPS_PER_THREAD):
        key = f"stress:{thread_id}:{i}"
        value = random_string(32)
        r.set(key, value)
        if r.get(key) != value:
            raise RuntimeError("Data mismatch detected!")


def test_concurrency():
    print(f"[TEST] Concurrency stress ({THREADS} threads x {OPS_PER_THREAD} ops)")

    start = time.time()
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(stress_worker, i) for i in range(THREADS)]
        for f in futures:
            f.result()  # raises if worker threw an exception

    duration = time.time() - start
    total_ops = THREADS * OPS_PER_THREAD
    ops_sec = total_ops / duration

    print("  ✔ Stress test passed")
    print(f"  Total ops: {total_ops}")
    print(f"  Duration: {duration:.2f}s")
    print(f"  Throughput: {ops_sec:,.0f} ops/sec")


def benchmark(r):
    print("[TEST] Simple throughput benchmark")

    n = 20000
    start = time.time()
    for i in range(n):
        r.set(f"bench:{i}", i)

    duration = time.time() - start
    print(f"  SET x {n} in {duration:.2f}s → {n/duration:,.0f} ops/sec")


def main() -> int:
    try:
        r = connect()
        print("✔ Connected to peadb")

        test_basic(r)
        test_expiry(r)
        test_atomic_incr(r)
        test_pipeline(r)
        test_concurrency()
        benchmark(r)

        print("\nALL TESTS PASSED")
        return 0
    except Exception as e:
        print("\nTEST FAILED:", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
