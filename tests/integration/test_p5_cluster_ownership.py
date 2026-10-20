#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready


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
    a = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6315", "--bind", "127.0.0.1",
                          "--cluster-enabled", "yes", "--cluster-announce-ip", "127.0.0.1",
                          "--loglevel", "warning"])
    b = subprocess.Popen([str(ROOT / "peadb-server"), "--port", "6316", "--bind", "127.0.0.1",
                          "--cluster-enabled", "yes", "--cluster-announce-ip", "127.0.0.1",
                          "--loglevel", "warning"])
    try:
        wait_until_ready(6315, process=a)
        wait_until_ready(6316, process=b)
        with socket.create_connection(("127.0.0.1", 6315), timeout=5) as s:
            assert cmd(s, "FLUSHALL") == "OK"
            myid = cmd(s, "CLUSTER", "MYID")
            assert len(myid) == 40 and all(c in "0123456789abcdef" for c in myid), myid
            # No slots assigned yet: CLUSTER SLOTS is empty and the state is not ok.
            assert cmd(s, "CLUSTER", "SLOTS") == []
            info0 = cmd(s, "CLUSTER", "INFO")
            assert "cluster_slots_assigned:0" in info0, info0
            assert "cluster_state:fail" in info0, info0
            # Assign the whole space to self the Redis way.
            assert cmd(s, "CLUSTER", "ADDSLOTSRANGE", "0", "16383") == "OK"
            info1 = cmd(s, "CLUSTER", "INFO")
            assert "cluster_slots_assigned:16384" in info1, info1
            assert "cluster_state:ok" in info1, info1
            # Re-adding an owned slot is an error in Redis.
            r = cmd(s, "CLUSTER", "ADDSLOTS", "0")
            assert isinstance(r, tuple) and r[0] == "ERR", r
            # An unknown node is refused, with Redis's wording.
            r = cmd(s, "CLUSTER", "SETSLOT", "0", "NODE", "nosuchnode")
            assert r == ("ERR", "ERR Unknown node nosuchnode"), r
            # CLUSTER MEET to this node's own announced address must be refused rather than
            # hang the server: try_send_cluster_meetback does a synchronous connect()/read()
            # on the single event-loop thread, so meeting self would deadlock (this node trying
            # to connect() to its own listening socket while it is the only thing that could
            # ever accept() the connection) rather than merely time out.
            r = cmd(s, "CLUSTER", "MEET", "127.0.0.1", "6315")
            assert isinstance(r, tuple) and r[0] == "ERR" and "MEET self" in r[1], r

            # CLUSTER MEET to an unreachable address must return promptly (bounded by a socket
            # timeout) rather than hang indefinitely, and must not block a subsequent command.
            t0 = time.monotonic()
            assert cmd(s, "CLUSTER", "MEET", "10.255.255.1", "7999") == "OK"
            elapsed = time.monotonic() - t0
            assert elapsed < 5.0, f"MEET to an unreachable address took {elapsed:.2f}s"
            assert cmd(s, "PING") == "PONG"

            # Register a peer, all slots still owned by self at this point.
            assert cmd(s, "CLUSTER", "MEET", "127.0.0.1", "6316") == "OK"

            # SETSLOT ... MIGRATING must not transfer reported ownership before any key has
            # actually moved (final review finding: it used to write the same array
            # cluster_owned_ranges() reads for CLUSTER SLOTS/SHARDS/NODES/cluster_size, so a
            # client refreshing its slot map would route slot-100 traffic to the peer and get
            # misses instead of ASK). Self must still show slot 100 as owned, cluster_size must
            # stay 1, and ASK must still work for a missing key in that slot.
            migrating_slot = 100
            slot100key = None
            for i in range(20000):
                k = f"mig{i}"
                if cmd(s, "CLUSTER", "KEYSLOT", k) == migrating_slot:
                    slot100key = k
                    break
            assert slot100key is not None, "could not find a key hashing to slot 100"
            assert cmd(s, "CLUSTER", "SETSLOT", str(migrating_slot), "MIGRATING",
                       "127.0.0.1:6316") == "OK"
            nodes_migrating = cmd(s, "CLUSTER", "NODES")
            self_line = [ln for ln in nodes_migrating.split("\n") if "myself" in ln][0]
            # Self's own slot ranges still include the whole space (nothing has been
            # reassigned yet), and the migrating marker names the peer.
            assert "0-16383" in self_line, self_line
            assert "[100->-" in self_line, self_line
            slots_migrating = cmd(s, "CLUSTER", "SLOTS")
            owning_ranges = [r for r in slots_migrating if r[0] <= migrating_slot <= r[1]]
            assert len(owning_ranges) == 1, slots_migrating
            assert owning_ranges[0][2][0] == "127.0.0.1" and owning_ranges[0][2][1] == 6315, \
                slots_migrating
            info_migrating = cmd(s, "CLUSTER", "INFO")
            assert "cluster_size:1" in info_migrating, info_migrating
            r = cmd(s, "GET", slot100key)
            assert isinstance(r, tuple) and r[1] == "ASK 100 127.0.0.1:6316", r
            # A definitive SETSLOT ... NODE ends the migration: the stale marker must not
            # linger on self's own NODES line once the slot has actually moved.
            assert cmd(s, "CLUSTER", "SETSLOT", str(migrating_slot), "NODE",
                       "127.0.0.1:6316") == "OK"
            nodes_after_node = cmd(s, "CLUSTER", "NODES")
            self_line_after = [ln for ln in nodes_after_node.split("\n") if "myself" in ln][0]
            assert "[100->-" not in self_line_after, self_line_after
            r = cmd(s, "GET", slot100key)
            assert isinstance(r, tuple) and r[1] == "MOVED 100 127.0.0.1:6316", r

            # Hand slot 0 to a real peer and check MOVED names it.
            assert cmd(s, "CLUSTER", "SETSLOT", "0", "NODE", "127.0.0.1:6316") == "OK"
            slot0key = "06S"  # CLUSTER KEYSLOT '06S' == 0
            assert cmd(s, "CLUSTER", "KEYSLOT", slot0key) == 0
            r = cmd(s, "GET", slot0key)
            assert isinstance(r, tuple) and r[1] == "MOVED 0 127.0.0.1:6316", r
            # A node id token (not just host:port) also resolves — round-trip via NODES.
            nodes = cmd(s, "CLUSTER", "NODES")
            assert "127.0.0.1:6316" in nodes, nodes

            # COUNTKEYSINSLOT / GETKEYSINSLOT are pure functions of the keyspace.
            assert cmd(s, "SET", "x", "1") == "OK"
            xs = cmd(s, "CLUSTER", "KEYSLOT", "x")
            assert cmd(s, "CLUSTER", "COUNTKEYSINSLOT", str(xs)) == 1
            assert cmd(s, "CLUSTER", "GETKEYSINSLOT", str(xs), "10") == ["x"]

            # Regression coverage for the for_each_key-based rewrite: COUNTKEYSINSLOT and
            # GETKEYSINSLOT must keep agreeing with an independently computed histogram
            # (KEYS * + CLUSTER KEYSLOT per key, a code path neither subcommand under test
            # goes through) over a keyspace with varied names, plus a key that is about to
            # expire. The expiring key must vanish from KEYS/DBSIZE and from both slot
            # commands together -- keys("*")'s expiry handling is the correctness bar the
            # new for_each_key() walk has to match exactly, not just a speed target.
            assert cmd(s, "FLUSHALL") == "OK"
            nkeys = 300
            for i in range(nkeys):
                assert cmd(s, "SET", f"bulkkey:{i}", "v") == "OK"
            # A hash tag forces a same-slot collision so the maxkeys cap has something to
            # cap (with unclustered names alone, 300 keys rarely collide in the same slot
            # out of 16384).
            same_slot_keys = [f"{{sameslot}}:{i}" for i in range(5)]
            for k in same_slot_keys:
                assert cmd(s, "SET", k, "v") == "OK"
            nkeys += len(same_slot_keys)
            # A key that is about to expire: present right now, gone by the time we check.
            # A GET after the TTL passes forces a deterministic lazy reap (DataStore::get()
            # erases past-TTL entries on access) rather than relying on the active-expire
            # cron's random-bucket, budget-64-per-tick sweep to have found this one key by
            # chance -- that budget is unrelated to this change and not something this test
            # should depend on the timing of. Once reaped, DBSIZE (a raw map-size read,
            # unlike KEYS/for_each_key) is directly comparable to KEYS/for_each_key again.
            assert cmd(s, "SET", "expiring:soon", "v") == "OK"
            assert cmd(s, "PEXPIRE", "expiring:soon", "50") == 1
            time.sleep(0.2)
            assert cmd(s, "GET", "expiring:soon") is None

            dbsize = cmd(s, "DBSIZE")
            all_keys = cmd(s, "KEYS", "*")
            assert "expiring:soon" not in all_keys, all_keys
            assert dbsize == len(all_keys) == nkeys, (dbsize, len(all_keys), nkeys)

            histogram = {}
            for k in all_keys:
                slot = cmd(s, "CLUSTER", "KEYSLOT", k)
                histogram.setdefault(slot, []).append(k)
            assert len(histogram) > 1, histogram
            for slot, expected_keys in histogram.items():
                got_count = cmd(s, "CLUSTER", "COUNTKEYSINSLOT", str(slot))
                assert got_count == len(expected_keys), (slot, got_count, expected_keys)
                got_keys = cmd(s, "CLUSTER", "GETKEYSINSLOT", str(slot), "1000")
                assert sorted(got_keys) == sorted(expected_keys), (slot, got_keys, expected_keys)

            # A slot with zero keys (which also would have held the now-expired key, had it
            # not expired) reports 0 / [] cleanly.
            empty_slot = next(s2 for s2 in range(16384) if s2 not in histogram)
            assert cmd(s, "CLUSTER", "COUNTKEYSINSLOT", str(empty_slot)) == 0
            assert cmd(s, "CLUSTER", "GETKEYSINSLOT", str(empty_slot), "10") == []

            # The forced collision slot exercises the maxkeys cap: exactly 5 keys land
            # there; capping to 1 returns exactly one of them, and the count is unaffected
            # by the cap.
            sameslot = cmd(s, "CLUSTER", "KEYSLOT", same_slot_keys[0])
            assert cmd(s, "CLUSTER", "COUNTKEYSINSLOT", str(sameslot)) == len(same_slot_keys)
            capped = cmd(s, "CLUSTER", "GETKEYSINSLOT", str(sameslot), "1")
            assert len(capped) == 1 and capped[0] in same_slot_keys, capped
            # maxkeys=0 is a boundary Redis handles explicitly (numkeys = min(maxkeys,
            # keys_in_slot), cluster.c:6261-6262): even a slot with keys returns an empty
            # array, not an error and not the uncapped set.
            assert cmd(s, "CLUSTER", "GETKEYSINSLOT", str(sameslot), "0") == []

            # DELSLOTS drops ownership again.
            assert cmd(s, "CLUSTER", "DELSLOTSRANGE", "1", "16383") == "OK"
            assert "cluster_state:fail" in cmd(s, "CLUSTER", "INFO")
            # Deleting an already-unassigned slot is an error, matching Redis.
            r = cmd(s, "CLUSTER", "DELSLOTS", "5")
            assert isinstance(r, tuple) and r[0] == "ERR", r
        print("P5 cluster ownership tests passed")
        return 0
    finally:
        for p in (a, b):
            p.terminate()
            p.wait(timeout=10)


if __name__ == "__main__":
    raise SystemExit(main())
