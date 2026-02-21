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
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6509", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.25)
        with socket.create_connection(("127.0.0.1", 6509), timeout=2) as s:
            assert cmd(s, "DEL", "myset") == 0
            assert cmd(s, "SADD", "myset", "1", "2", "3", "4", "10") == 5
            out = cmd(s, "EVAL", "return redis.call('sort',KEYS[1],'desc')", "1", "myset")
            assert out == ["10", "4", "3", "2", "1"]

            assert cmd(s, "DEL", "myset") == 1
            assert cmd(s, "SADD", "myset", "a", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "z", "aa", "aaa", "azz") == 24
            out = cmd(s, "EVAL", "return redis.call('sort',KEYS[1],'by','_')", "1", "myset")
            assert out == ["a", "aa", "aaa", "azz", "b", "c", "d", "e", "f", "g", "h", "i", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "z"]

            assert cmd(s, "DEL", "myset") == 1
            assert cmd(s, "SADD", "myset", "a", "b", "c") == 3
            out = cmd(s, "EVAL", "return redis.call('sort',KEYS[1],'by','_','get','#','get','_:*')", "1", "myset")
            assert out == ["a", None, "b", None, "c", None]

        print("P2 sort-in-script tests passed")
        return 0
    finally:
        p.terminate(); p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())
