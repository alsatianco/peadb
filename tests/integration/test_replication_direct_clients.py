#!/usr/bin/env python3
"""Swap publication invalidates all WATCH clients and rechecks blockers only against new data."""
from _replication_direct import (MARK, Master, PEADB, encode, header, info, loading,
    reference, seed_old, server, snapshot, wait_for, wait_link)
from test_rdb_redis_interop import Conn


def run(binary, rdb):
    clients = []
    with server(binary) as (r, port, _, _), Master() as m:
        try:
            seed_old(r)
            # Identical payloads on both sides must still invalidate WATCH at exchange.
            assert r('SET', 'padding:0', 'p' * 512) == '+OK'
            watcher, missing = Conn(port), Conn(port)
            clients += [watcher, missing]
            assert watcher('WATCH', 'padding:0') == '+OK'
            assert watcher('SELECT', 15) == '+OK'
            assert watcher('WATCH', 'local') == '+OK'
            assert watcher('SELECT', 0) == '+OK'
            assert missing('WATCH', 'still-missing') == '+OK'
            old_id = info(r, 'master_replid')
            assert r('CONFIG', 'SET', 'replica-read-only', 'no') == '+OK'
            assert r('CONFIG', 'SET', 'repl-diskless-load', 'swapdb') == '+OK'
            assert r('CONFIG', 'SET', 'key-load-delay', 20000) == '+OK'
            assert r('CONFIG', 'SET', 'loading-process-events-interval-bytes', 1024) == '+OK'
            assert r('REPLICAOF', '127.0.0.1', m.port) == '+OK'
            sock, _, _ = m.accept()
            header(sock, old_id, rdb, False)
            sock.sendall(rdb + MARK)
            wait_for(lambda: loading(r, True), 'matching-ID old-data service')
            # Newly accepted sessions during the pump must also be collected at publication.
            late = Conn(port); clients.append(late)
            assert late('WATCH', 'padding:0') == '+OK'
            assert r('SET', 'local-during-load', 'local', 'PX', 60000) == '+OK'
            assert int(info(r, 'slave_expires_tracked_keys', 'stats')) >= 1
            requests = [
                ('BLPOP', 'L', 0), ('BZPOPMIN', 'Z', 0),
                ('XREAD', 'BLOCK', 0, 'STREAMS', 'events', '0-0'),
                ('XREADGROUP', 'GROUP', 'old-group', 'c', 'BLOCK', 0, 'STREAMS', 'gone', '>')]
            blockers = []
            for command in requests:
                c = Conn(port); clients.append(c); blockers.append(c)
                c.sock.sendall(encode(*command) + encode('PING'))
            wait_for(lambda: info(r, 'blocked_clients', 'clients') == '4', 'all load-time blockers')
            wait_link(r)
            assert blockers[0].read() == [b'L', b'from-snapshot']
            assert blockers[1].read() == [b'Z', b'member', b'1']
            assert blockers[2].read() == [[b'events', [[b'2-0', [b'field', b'next']]]]]
            assert blockers[3].read().startswith('-NOGROUP')
            for c in blockers:
                assert c.read() == '+PONG', 'released pipeline must run after its blocker'
            for c in [watcher, late]:
                assert c('MULTI') == '+OK'
                assert c('GET', 'padding:0') == '+QUEUED'
                assert c('EXEC') is None, 'replacement invalidates identical-value WATCH'
            assert missing('MULTI') == '+OK'
            assert missing('PING') == '+QUEUED'
            assert missing('EXEC') == ['+PONG']
            assert info(r, 'blocked_clients', 'clients') == '0'
            assert info(r, 'slave_expires_tracked_keys', 'stats') == '0'
            assert r('GET', 'local-during-load') is None
            print('PASS load-time clients, WATCH, blockers and local TTL ownership', binary, flush=True)
        finally:
            for c in clients:
                c.close()


def main():
    rdb, _, _ = snapshot()
    for binary in [reference(), PEADB]:
        run(binary, rdb)


if __name__ == '__main__':
    main()
