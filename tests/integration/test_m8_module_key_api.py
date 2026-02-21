#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


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
    if p == b"+":
        return rl(s).decode()
    if p == b"-":
        return ("ERR", rl(s).decode())
    if p == b":":
        return int(rl(s))
    if p == b"$":
        n = int(rl(s))
        if n == -1:
            return None
        v = rx(s, n)
        rx(s, 2)
        return v.decode()
    if p == b"*":
        n = int(rl(s))
        if n == -1:
            return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)


def cmd(s, *a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode()
        d += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(d)
    return recv(s)


def main():
    with tempfile.TemporaryDirectory(prefix="m8key_") as td:
        mod_cpp = pathlib.Path(td) / "key_module.cpp"
        mod_so = pathlib.Path(td) / "key_module.so"
        mod_cpp.write_text(
            """
            #include <cstddef>
            extern "C" void* RedisModule_OpenKey(const char*);
            extern "C" int RedisModule_StringSet(void*, const char*);
            extern "C" const char* RedisModule_StringDMA(void*, size_t*, int);
            extern "C" int RedisModule_OnLoad(void*, void*, int) {
              void* k = RedisModule_OpenKey("m8:key");
              if (!k) return 1;
              if (RedisModule_StringSet(k, "m8-value") != 0) return 1;
              size_t n = 0;
              const char* p = RedisModule_StringDMA(k, &n, 0);
              if (!p || n != 8) return 1;
              return 0;
            }
            """,
            encoding="utf-8",
        )
        subprocess.check_call(["g++", "-shared", "-fPIC", "-O2", str(mod_cpp), "-o", str(mod_so)])

        p = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6434", "--bind", "127.0.0.1", "--loglevel", "error"])
        try:
            time.sleep(0.2)
            with socket.create_connection(("127.0.0.1", 6434), timeout=2) as s:
                assert cmd(s, "FLUSHALL") == "OK"
                assert cmd(s, "MODULE", "LOAD", str(mod_so)) == "OK"
                assert cmd(s, "GET", "m8:key") == "m8-value"
            print("M8 module key API tests passed")
            return 0
        finally:
            p.terminate()
            p.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())
