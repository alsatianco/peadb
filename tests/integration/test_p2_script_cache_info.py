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
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s, *a):
    s.sendall(enc(*a))
    return recv(s)


def main():
    p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6508", "--bind", "127.0.0.1", "--loglevel", "error"])
    try:
        time.sleep(0.25)
        with socket.create_connection(("127.0.0.1", 6508), timeout=2) as s:
            assert cmd(s, "SCRIPT", "FLUSH") == "OK"
            for j in range(100):
                sha = cmd(s, "SCRIPT", "LOAD", f"return {j}")
                assert isinstance(sha, str) and len(sha) == 40

            mem = cmd(s, "INFO", "MEMORY")
            assert "number_of_cached_scripts:100" in mem

            assert cmd(s, "SCRIPT", "FLUSH", "ASYNC") == "OK"
            mem = cmd(s, "INFO", "MEMORY")
            assert "number_of_cached_scripts:0" in mem

            assert cmd(s, "EVAL", "return 1+1", "0") == 2
            ex = cmd(s, "SCRIPT", "EXISTS", "a27e7e8a43702b7046d4f6a7ccf5b60cef6b9bd9", "a27e7e8a43702b7046d4f6a7ccf5b60cef6b9bda")
            assert ex == [1, 0]

        print("P2 script cache/info tests passed")
        return 0
    finally:
        p.terminate()
        p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())
