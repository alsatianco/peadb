#!/usr/bin/env python3
import pathlib, socket, subprocess, time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def enc(*a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return d


def rx(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise RuntimeError("closed")
        b += c
    return b


def rl(s):
    b = b""
    while not b.endswith(b"\r\n"):
        b += rx(s, 1)
    return b[:-2]


def recv(s):
    p = rx(s, 1)
    if p == b'+': return rl(s).decode()
    if p == b'-': return ("ERR", rl(s).decode())
    if p == b':': return int(rl(s))
    if p == b'$':
        n = int(rl(s))
        if n == -1: return None
        d = rx(s, n); rx(s, 2); return d.decode()
    if p == b'*':
        n = int(rl(s))
        if n == -1: return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s, *a):
    s.sendall(enc(*a)); return recv(s)


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6512", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.25)
        with socket.create_connection(("127.0.0.1", 6512), timeout=2) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            assert cmd(s, "SADD", "myset", "a", "b", "c") == 3
            assert cmd(s, "EVAL", "return redis.call('spop', 'myset')", "0") is not None
            v = cmd(s, "EVAL", "return redis.call('spop', 'myset', 1)", "0")
            assert isinstance(v, list) and len(v) == 1
            assert cmd(s, "EVAL", "return redis.call('spop', KEYS[1])", "1", "myset") is not None
            assert cmd(s, "EVAL", "return redis.call('spop', KEYS[1])", "1", "myset") is None

            assert cmd(s, "MSET", "a{t}", "1", "b{t}", "2", "c{t}", "3", "d{t}", "4") == "OK"
            assert cmd(s, "EVAL", "return redis.call('mget', 'a{t}', 'b{t}', 'c{t}', 'd{t}')", "0") == ["1", "2", "3", "4"]

            assert cmd(s, "SET", "expirekey", "1") == "OK"
            assert cmd(s, "EVAL", "return redis.call('expire', KEYS[1], ARGV[1])", "1", "expirekey", "3") == 1

            hm = cmd(s, "EVAL", "redis.call('hmget', KEYS[1], 1, 2, 3)", "1", "key")
            assert hm is None
            assert cmd(s, "EVAL", "redis.call('incrbyfloat', KEYS[1], 1)", "1", "key") is None
            assert cmd(s, "EVAL", "redis.call('set', KEYS[1], '1', 'KEEPTTL')", "1", "key") is None
            assert cmd(s, "GET", "key") == "1"

        print("P2 script rewrite-ops tests passed")
        return 0
    finally:
        p.terminate(); p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())
