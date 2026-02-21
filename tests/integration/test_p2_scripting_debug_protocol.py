#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def enc(*args: str) -> bytes:
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return out


def rx(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise RuntimeError("connection closed")
        buf += chunk
    return buf


def rl(sock: socket.socket) -> bytes:
    b = b""
    while not b.endswith(b"\r\n"):
        b += rx(sock, 1)
    return b[:-2]


def read_reply(sock: socket.socket):
    p = rx(sock, 1)

    if p in (b"+", b"-", b":", b",", b"(", b"#", b"_"):
        line = rl(sock).decode(errors="replace")
        head = p.decode() + line
        if p == b"+":
            return head, line
        if p == b"-":
            return head, ("ERR", line)
        if p == b":":
            return head, int(line)
        if p == b",":
            return head, float(line)
        if p == b"(":
            return head, line
        if p == b"#":
            return head, line == "t"
        return head, None

    if p == b"$":
        n = int(rl(sock))
        head = f"${n}"
        if n == -1:
            return head, None
        payload = rx(sock, n)
        rx(sock, 2)
        return head, payload.decode(errors="replace")

    if p == b"=":
        n = int(rl(sock))
        head = f"={n}"
        payload = rx(sock, n)
        rx(sock, 2)
        return head, payload.decode(errors="replace")

    if p == b"*":
        n = int(rl(sock))
        head = f"*{n}"
        if n == -1:
            return head, None
        return head, [read_reply(sock)[1] for _ in range(n)]

    if p == b"%":
        n = int(rl(sock))
        head = f"%{n}"
        m = {}
        for _ in range(n):
            k = read_reply(sock)[1]
            v = read_reply(sock)[1]
            m[k] = v
        return head, m

    if p == b"~":
        n = int(rl(sock))
        head = f"~{n}"
        return head, [read_reply(sock)[1] for _ in range(n)]

    if p == b"|":
        n = int(rl(sock))
        for _ in range(n):
            read_reply(sock)
            read_reply(sock)
        return read_reply(sock)

    raise RuntimeError(f"unsupported prefix: {p!r}")


def cmd(sock: socket.socket, *args: str):
    sock.sendall(enc(*args))
    return read_reply(sock)


def eval_head(sock: socket.socket, script: str):
    return cmd(sock, "EVAL", script, "0")


def main() -> int:
    port = 6505
    proc = subprocess.Popen(
        [str(ROOT / "peadb-server"), "--port", str(port), "--bind", "127.0.0.1", "--loglevel", "error"]
    )
    try:
        time.sleep(0.25)
        with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
            assert cmd(s, "DEBUG", "SET-DISABLE-DENY-SCRIPTS", "1")[1] == "OK"

            big = "1234567999999999999999999999999999999"
            for script_resp in (2, 3):
                for client_resp in (2, 3):
                    assert cmd(s, "HELLO", str(client_resp))[0].startswith(("*", "%"))

                    is_resp2_out = client_resp == 2 or script_resp == 2

                    h, v = eval_head(s, f"redis.setresp({script_resp});return redis.call('debug', 'protocol', 'bignum')")
                    assert h == ("$37" if is_resp2_out else "(" + big)
                    if is_resp2_out:
                        assert v == big

                    h, v = eval_head(s, "return {big_number='123\\r\\n123'}")
                    assert h == ("$8" if client_resp == 2 else "(123  123")
                    if client_resp == 2:
                        assert v == "123  123"

                    h, _ = eval_head(s, f"redis.setresp({script_resp});return redis.call('debug', 'protocol', 'map')")
                    assert h == ("*6" if is_resp2_out else "%3")

                    h, _ = eval_head(s, f"redis.setresp({script_resp});return redis.call('debug', 'protocol', 'set')")
                    assert h == ("*3" if is_resp2_out else "~3")

                    h, v = eval_head(s, f"redis.setresp({script_resp});return redis.call('debug', 'protocol', 'double')")
                    assert h == ("$5" if is_resp2_out else ",3.141")
                    if is_resp2_out:
                        assert v == "3.141"

                    h, _ = eval_head(s, f"redis.setresp({script_resp});return redis.call('debug', 'protocol', 'null')")
                    assert h == ("$-1" if client_resp == 2 else "_")

                    h, v = eval_head(s, f"redis.setresp({script_resp});return redis.call('debug', 'protocol', 'verbatim')")
                    assert h == ("$25" if is_resp2_out else "=29")
                    if is_resp2_out:
                        assert v == "This is a verbatim\nstring"
                    else:
                        assert v == "txt:This is a verbatim\nstring"

                    h, _ = eval_head(s, f"redis.setresp({script_resp});return redis.call('debug', 'protocol', 'true')")
                    assert h == (":1" if is_resp2_out else "#t")

                    h, _ = eval_head(s, f"redis.setresp({script_resp});return redis.call('debug', 'protocol', 'false')")
                    assert h == (":0" if is_resp2_out else "#f")

            cmd(s, "HELLO", "3")
            h, v = eval_head(s, "redis.setresp(3);return redis.call('debug', 'protocol', 'attrib')")
            assert h.startswith("$") and v == "Some real reply following the attribute"

            lib = (
                "#!lua name=dbg\n"
                "redis.register_function('attrib', function(KEYS, ARGV)\n"
                "  redis.setresp(3)\n"
                "  return redis.call('debug', 'protocol', 'attrib')\n"
                "end)"
            )
            assert cmd(s, "FUNCTION", "FLUSH")[1] == "OK"
            assert cmd(s, "FUNCTION", "LOAD", "REPLACE", lib)[1] == "dbg"
            h, v = cmd(s, "FCALL", "attrib", "0")
            assert h.startswith("$") and v == "Some real reply following the attribute"

        print("P2 scripting debug-protocol matrix tests passed")
        return 0
    finally:
        proc.terminate()
        proc.wait(timeout=3)


if __name__ == "__main__":
    raise SystemExit(main())
