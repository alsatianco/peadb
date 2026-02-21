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
    if p == b'+':
        return rl(s).decode()
    if p == b'-':
        return ("ERR", rl(s).decode())
    if p == b':':
        return int(rl(s))
    if p == b'$':
        n = int(rl(s))
        if n == -1:
            return None
        d = rx(s, n)
        rx(s, 2)
        return d.decode()
    if p == b'*':
        n = int(rl(s))
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s, *a):
    s.sendall(enc(*a))
    return recv(s)


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6507", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.25)
        with socket.create_connection(("127.0.0.1", 6507), timeout=2) as s:
            script = "return redis.call('get',KEYS[1])"
            sha = cmd(s, "SCRIPT", "LOAD", script)
            assert sha == "fd758d1589d044dd850a6f05d52f2eefd27f033f"
            assert cmd(s, "SET", "mykey", "myval") == "OK"
            assert cmd(s, "EVALSHA", sha, "1", "mykey") == "myval"
            assert cmd(s, "EVALSHA", sha.upper(), "1", "mykey") == "myval"
            assert cmd(s, "SCRIPT", "EXISTS", sha, sha.upper()) == [1, 1]
        print("P2 evalsha sha1 tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())
