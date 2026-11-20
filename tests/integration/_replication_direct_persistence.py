"""Real-engine direct-load persistence and chained-history acceptance."""
import subprocess

from _readiness import wait_until_ready
from _replication_snapshot import VALUE, seed, verify
from _replication_direct import PEADB, info, reference, seed_old, server, wait_for, wait_link
from test_rdb_redis_interop import Conn
from test_replica_offset_parity import stop


def aof_ready(c):
    def finished():
        # A scheduled rewrite can start between two INFO requests. Observe both flags
        # together, so scheduled=0 cannot accidentally pair with the earlier child=0.
        state = dict(line.split(':', 1) for line in c('INFO', 'persistence').decode().splitlines()
                     if ':' in line and not line.startswith('#'))
        return state['aof_rewrite_in_progress'] == '0' and state['aof_rewrite_scheduled'] == '0'
    wait_for(finished, 'replacement AOF durable')
    assert info(c, 'aof_last_bgrewrite_status', 'persistence') == 'ok'


def restart(binary, port, directory, verify_data):
    with (directory / 'restart.log').open('ab') as log:
        proc = subprocess.Popen([binary, '--bind', '127.0.0.1', '--port', str(port),
            '--dir', str(directory), '--save', '', '--appendonly', 'yes',
            '--appendfsync', 'always', '--shutdown-timeout', '0'], stdout=log, stderr=subprocess.STDOUT)
    client = None
    try:
        wait_until_ready(port, process=proc)
        client = Conn(port)
        verify_data(client)
    finally:
        if client:
            client.close()
        stop(proc)
        proc.wait(timeout=10)


def success(master_binary, replica_binary, diskless):
    with (server(master_binary) as (m, pm, _, _),
          server(replica_binary) as (r, pr, td, proc),
          server(reference()) as (chain, _, _, _)):
        deadline = seed(m)
        assert m('RPUSH', 'list', 'a', 'b') == 2
        assert m('HSET', 'hash', 'f', 'value') == 1
        assert m('SADD', 'set', 'a', 'b') == 2
        assert m('ZADD', 'zset', 1, 'a', 2, 'b') == 2
        assert m('XADD', 'events', '1-0', 'f', 'v') == b'1-0'
        assert m('XGROUP', 'CREATE', 'events', 'g', 0) == '+OK'
        assert m('XREADGROUP', 'GROUP', 'g', 'consumer', 'STREAMS', 'events', '>')
        original_stream = m('XINFO', 'STREAM', 'events', 'FULL')
        assert m('CONFIG', 'SET', 'repl-diskless-sync', 'yes' if diskless else 'no') == '+OK'
        seed_old(r)
        for index in range(64):
            assert r('SET', f'obsolete:{index}', 'old') == '+OK'
        assert r('SAVE') == '+OK'
        original_file = (td / 'dump.rdb').read_bytes()
        assert r('CONFIG', 'SET', 'appendfsync', 'always') == '+OK'
        assert r('CONFIG', 'SET', 'appendonly', 'yes') == '+OK'
        aof_ready(r)
        assert r('CONFIG', 'SET', 'rdb-key-save-delay', 100000) == '+OK'
        assert r('BGSAVE').startswith('+Background saving')
        assert info(r, 'rdb_bgsave_in_progress', 'persistence') == '1'
        assert r('CONFIG', 'SET', 'repl-diskless-load', 'swapdb') == '+OK'
        assert r('REPLICAOF', '127.0.0.1', pm) == '+OK'
        wait_link(r)
        wait_for(lambda: info(r, 'rdb_bgsave_in_progress', 'persistence') == '0', 'old child reaped')
        verify(r, deadline)
        assert r('XINFO', 'STREAM', 'events', 'FULL') == original_stream
        assert (td / 'dump.rdb').read_bytes() == original_file
        assert not list(td.glob('temp-*.rdb'))
        assert chain('REPLICAOF', '127.0.0.1', pr) == '+OK'
        wait_link(chain)
        assert m('SELECT', 5) == '+OK'
        assert m('INCR', 'stream-counter') == 1
        assert m('WAIT', 1, 5000) == 1
        assert r('SELECT', 5) == '+OK'
        wait_for(lambda: r('GET', 'stream-counter') == b'1', 'direct master stream')
        assert chain('SELECT', 5) == '+OK'
        wait_for(lambda: chain('GET', 'stream-counter') == b'1', 'chained raw stream')
        assert r('SELECT', 0) == '+OK'
        assert chain('SELECT', 0) == '+OK'
        old_id = info(r, 'master_replid')
        assert r('REPLICAOF', 'NO', 'ONE') == '+OK'
        assert info(r, 'master_replid2') == old_id
        assert r('SET', 'promoted', 'local') == '+OK'
        wait_for(lambda: chain('GET', 'promoted') == b'local', 'chain after promotion')
        aof_ready(r)
        proc.kill(); assert proc.wait(timeout=5) != 0

        def verify_restart(c):
            verify(c, deadline)
            assert c('GET', 'promoted') == b'local'
            assert c('GET', 'old') is None
            assert c('LRANGE', 'list', 0, -1) == [b'a', b'b']
            assert c('HGET', 'hash', 'f') == b'value'
            assert sorted(c('SMEMBERS', 'set')) == [b'a', b'b']
            assert c('ZRANGE', 'zset', 0, -1, 'WITHSCORES') == [b'a', b'1', b'b', b'2']
            assert c('XPENDING', 'events', 'g')[0] == 1
            assert c('SELECT', 5) == '+OK'
            assert c('GET', 'stream-counter') == b'1'
        restart(replica_binary, pr, td, verify_restart)
        print('PASS direct real pairing/AOF/crash/child/chain/promotion',
              master_binary, replica_binary, diskless, flush=True)
