#!/usr/bin/env python3
"""Pins the RESP3 verbatim-string reply shape (7d382c2) on the raw wire.

RESP2 replies from the affected commands must still start with '$' (a plain bulk string).
RESP3 replies (after HELLO 3) must start with '=' (a verbatim string), and the payload itself
must carry the leading 'txt:' format tag Redis's own addReplyVerbatim always emits -- the
length header counts that tag too. Both are read as raw bytes, never through a decoder that
would normalize '$' and '=' bulk/verbatim strings into the same Python value and so silently
tolerate a regression back to a plain bulk string under RESP3.

Covers the 8 commands genuinely unpinned by any other test: INFO, CLUSTER NODES, CLUSTER INFO,
CLIENT INFO, CLIENT LIST, MEMORY DOCTOR, LATENCY DOCTOR, LATENCY GRAPH.
  - DEBUG PROTOCOL verbatim is already pinned by test_p2_scripting_debug_protocol.py (a $25 vs
    =29 round trip via EVAL) -- not duplicated here.
  - MEMORY MALLOC-STATS is deliberately NOT verbatim, matching Redis's own non-jemalloc
    fallback (object.c:1663-1665); this test pins that it stays a plain bulk string under
    RESP3 too, as a guard against "fixing" it into a verbatim string by mistake.
"""
import pathlib
import socket
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready

PORT = 6318


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


def send(s, *args):
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    s.sendall(out)


def recv_generic(s):
    """RESP2/RESP3-aware decoder for replies whose *shape* this test does not care about --
    HELLO, CONFIG SET, DEBUG SLEEP -- never for the commands under test (those go through
    recv_raw below, which does not normalize '$' and '=' into the same thing)."""
    p = rx(s, 1)
    if p == b"+":
        return rl(s).decode()
    if p == b"-":
        return ("ERR", rl(s).decode())
    if p == b":":
        return int(rl(s))
    if p in (b"$", b"="):
        n = int(rl(s))
        if n == -1:
            return None
        v = rx(s, n)
        rx(s, 2)
        return v.decode()
    if p in (b"*", b">", b"~"):
        n = int(rl(s))
        if n == -1:
            return None
        return [recv_generic(s) for _ in range(n)]
    if p == b"%":
        n = int(rl(s))
        return {recv_generic(s): recv_generic(s) for _ in range(n)}
    if p == b"_":
        rl(s)
        return None
    if p == b"#":
        return rl(s) == b"t"
    if p == b",":
        return float(rl(s))
    raise RuntimeError(p)


def recv_raw(s):
    """Reads exactly one reply as (lead_byte, declared_length, raw_body_bytes) with NO
    type-specific interpretation. This is the actual pin: a caller must check `lead` itself
    rather than get a value a decoder already normalized away."""
    lead = rx(s, 1)
    if lead not in (b"$", b"="):
        raise AssertionError(f"expected a bulk ($) or verbatim (=) reply, got {lead!r}")
    n = int(rl(s))
    body = rx(s, n)
    rx(s, 2)
    return lead, n, body


def check_resp2_bulk(s, *args):
    send(s, *args)
    lead, n, body = recv_raw(s)
    label = " ".join(args)
    assert lead == b"$", f"{label}: RESP2 must reply with a plain bulk string ($), got {lead!r}"
    assert len(body) == n, (label, n, len(body))
    return body


def check_resp3_verbatim(s, *args):
    send(s, *args)
    lead, n, body = recv_raw(s)
    label = " ".join(args)
    assert lead == b"=", f"{label}: RESP3 must reply with a verbatim string (=), got {lead!r}"
    assert len(body) == n, (label, n, len(body))
    assert body.startswith(b"txt:"), (
        f"{label}: verbatim payload must carry the 'txt:' format tag, got {body[:8]!r}")
    return body


def check_resp2_bulk_not_verbatim(s, *args):
    """MEMORY MALLOC-STATS: must stay a plain bulk string even under RESP3 -- there is no
    allocator introspection to report, matching Redis's own non-jemalloc fallback."""
    send(s, *args)
    lead, _n, body = recv_raw(s)
    label = " ".join(args)
    assert lead == b"$", f"{label}: must stay a plain bulk string ($), got {lead!r}"
    return body


def main():
    p = subprocess.Popen([
        str(ROOT / "peadb-server"), "--port", str(PORT), "--bind", "127.0.0.1",
        "--cluster-enabled", "yes", "--loglevel", "warning",
    ])
    try:
        wait_until_ready(PORT)
        with socket.create_connection(("127.0.0.1", PORT), timeout=5) as s:
            # LATENCY GRAPH replies with a plain, empty bulk string (not verbatim) when there
            # is no recorded history for the event yet (command.cpp) -- seed one "command"
            # sample first so both the RESP2 and RESP3 checks below exercise the verbatim path.
            send(s, "CONFIG", "SET", "latency-monitor-threshold", "1")
            assert recv_generic(s) == "OK"
            send(s, "DEBUG", "SLEEP", "0.05")
            assert recv_generic(s) == "OK"

            check_resp2_bulk(s, "INFO", "server")
            check_resp2_bulk(s, "CLUSTER", "NODES")
            check_resp2_bulk(s, "CLUSTER", "INFO")
            check_resp2_bulk(s, "CLIENT", "INFO")
            check_resp2_bulk(s, "CLIENT", "LIST")
            memory_doctor_resp2 = check_resp2_bulk(s, "MEMORY", "DOCTOR")
            check_resp2_bulk(s, "LATENCY", "DOCTOR")
            check_resp2_bulk(s, "LATENCY", "GRAPH", "command")
            check_resp2_bulk_not_verbatim(s, "MEMORY", "MALLOC-STATS")

            send(s, "HELLO", "3")
            recv_generic(s)

            check_resp3_verbatim(s, "INFO", "server")
            check_resp3_verbatim(s, "CLUSTER", "NODES")
            check_resp3_verbatim(s, "CLUSTER", "INFO")
            check_resp3_verbatim(s, "CLIENT", "INFO")
            check_resp3_verbatim(s, "CLIENT", "LIST")
            memory_doctor_resp3 = check_resp3_verbatim(s, "MEMORY", "DOCTOR")
            check_resp3_verbatim(s, "LATENCY", "DOCTOR")
            check_resp3_verbatim(s, "LATENCY", "GRAPH", "command")
            check_resp2_bulk_not_verbatim(s, "MEMORY", "MALLOC-STATS")

            # MEMORY DOCTOR's text is a hardcoded literal (command.cpp) -- no content drift is
            # possible between the two calls, so this is the one command where the exact
            # "txt:" + RESP2-payload relationship can be pinned byte-for-byte, not just
            # structurally (the other commands' text can legitimately shift between the two
            # calls -- e.g. CLIENT INFO/LIST's own "resp=" field, or a new LATENCY sample
            # recorded by an intervening command -- so cross-call byte equality isn't a safe
            # assertion for those; the '$'/'='+'txt:' structural checks above already pin them).
            assert memory_doctor_resp3 == b"txt:" + memory_doctor_resp2, (
                memory_doctor_resp2, memory_doctor_resp3)

        print("P5 RESP3 verbatim reply shape pinned for INFO, CLUSTER NODES/INFO, "
              "CLIENT INFO/LIST, MEMORY DOCTOR, LATENCY DOCTOR/GRAPH -- passed")
    finally:
        p.terminate()
        p.wait(timeout=10)


main()
