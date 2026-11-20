#!/usr/bin/env python3
"""Production direct socket waiting obeys live timeout and prompt owned shutdown cancellation."""
import signal
import time

from _replication_direct import (Master, PEADB, REPLID, header, info, loading,
    seed_old, server, snapshot, wait_for)


def run(rdb, matching, shutdown):
    with server(PEADB) as (r, _, td, proc), Master() as m:
        seed_old(r)
        assert r('SAVE') == '+OK'
        original = (td / 'dump.rdb').read_bytes()
        old_id = info(r, 'master_replid')
        assert r('CONFIG', 'SET', 'repl-diskless-load', 'swapdb') == '+OK'
        assert r('CONFIG', 'SET', 'repl-timeout', 1) == '+OK'
        assert r('REPLICAOF', '127.0.0.1', m.port) == '+OK'
        sock, _, _ = m.accept()
        header(sock, old_id if matching else REPLID, rdb, False)
        sock.sendall(rdb[:100])
        wait_for(lambda: loading(r, matching), 'stalled production socket source service')
        if shutdown:
            started = time.monotonic()
            proc.send_signal(signal.SIGTERM)
            assert proc.wait(timeout=1.5) == 0
            assert time.monotonic() - started < 1.5
        else:
            m.accept()  # reconnect only after the bounded source failed and returned its lease
            assert r('GET', 'old') == b'old-value'
            assert r('FCALL_RO', 'old', 0) == 1
            assert info(r, 'master_replid') == old_id
        assert (td / 'dump.rdb').read_bytes() == original
        assert not list(td.glob('temp-*.rdb'))
        print('PASS direct source timeout/shutdown', matching, shutdown, flush=True)


def main():
    rdb, _, _ = snapshot()
    for matching in [False, True]:
        for shutdown in [False, True]:
            run(rdb, matching, shutdown)


if __name__ == '__main__':
    main()
