#!/usr/bin/env python3
"""CLUSTER SLOTS/SHARDS/NODES reply-shape parity with Redis 7.2 (cluster.c).

Exercises the announced address (never loopback) flowing through all three
replies, the 4-element CLUSTER SLOTS node entry (ip, port, id, empty
additional-endpoints map), the CLUSTER SHARDS 2-field/7-field map shape, and
the \\n-separated CLUSTER NODES line format. See
docs/decisions/p5-cluster-addressing-and-version-provenance.md.
"""
import pathlib
import socket
import subprocess
ROOT = pathlib.Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready


def rx(s, n):
    b = b''
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise RuntimeError('closed')
        b += c
    return b


def rl(s):
    b = b''
    while not b.endswith(b'\r\n'):
        b += rx(s, 1)
    return b[:-2]


def recv(s):
    p = rx(s, 1)
    if p == b'+':
        return rl(s).decode()
    if p == b'-':
        return ('ERR', rl(s).decode())
    if p == b':':
        return int(rl(s))
    if p == b'$':
        n = int(rl(s))
        if n == -1:
            return None
        v = rx(s, n)
        rx(s, 2)
        return v.decode()
    if p == b'*':
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
    p = subprocess.Popen([str(ROOT / 'peadb-server'), '--port', '6317', '--bind', '127.0.0.1',
                          '--cluster-enabled', 'yes', '--cluster-announce-ip', '10.9.8.7',
                          '--cluster-announce-port', '7100', '--loglevel', 'error'])
    try:
        wait_until_ready(6317)
        with socket.create_connection(('127.0.0.1', 6317), timeout=5) as s:
            myid = cmd(s, 'CLUSTER', 'MYID')
            assert cmd(s, 'CLUSTER', 'ADDSLOTSRANGE', '0', '16383') == 'OK'

            sl = cmd(s, 'CLUSTER', 'SLOTS')
            assert len(sl) == 1, sl
            assert sl[0][0] == 0 and sl[0][1] == 16383, sl
            node = sl[0][2]
            # Redis 7.2 sends FOUR elements per node: ip, port, id, additional-endpoints map.
            assert len(node) == 4, node
            assert node[0] == '10.9.8.7', node          # announced address, never loopback
            assert node[1] == 7100, node                # announced port
            assert node[2] == myid, node
            assert node[3] == [], node                  # empty map is *0 in RESP2

            sh = cmd(s, 'CLUSTER', 'SHARDS')
            assert len(sh) == 1, sh
            d = dict(zip(sh[0][0::2], sh[0][1::2]))
            assert d['slots'] == [0, 16383], d
            n = dict(zip(d['nodes'][0][0::2], d['nodes'][0][1::2]))
            assert n['id'] == myid and n['ip'] == '10.9.8.7' and n['port'] == 7100, n
            assert n['endpoint'] == '10.9.8.7' and n['role'] == 'master' and n['health'] == 'online', n
            assert isinstance(n['replication-offset'], int), n

            nodes = cmd(s, 'CLUSTER', 'NODES')
            assert '\r\n' not in nodes, 'CLUSTER NODES lines are \\n-separated in Redis'
            line = nodes.strip().split('\n')[0].split()
            assert line[0] == myid, line
            # This is the self ("myself,master") line, single-node cluster: guards against the
            # regression this whole task closes — the self line used to hardcode 127.0.0.1
            # regardless of --cluster-announce-ip, disagreeing with the node id it sits next to
            # (which was already derived from the real announced address).
            assert line[1] == '10.9.8.7:7100@17100', line
            assert 'myself,master' in line[2], line
            assert line[-1] == '0-16383', line

            info = cmd(s, 'CLUSTER', 'INFO')
            assert 'cluster_state:ok' in info, info
            assert 'cluster_slots_assigned:16384' in info, info
            assert 'cluster_size:1' in info, info
            assert 'cluster_known_nodes:1' in info, info
        print('P5 cluster reply tests passed')
        return 0
    finally:
        p.terminate()
        p.wait(timeout=10)


if __name__ == '__main__':
    raise SystemExit(main())
