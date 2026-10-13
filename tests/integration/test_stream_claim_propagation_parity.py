#!/usr/bin/env python3
"""Redis-first claim options, exact PEL replication and AOF replay (delta 48)."""
import io
import os
import pathlib
import socket
import tempfile
import time

from test_replica_gates_parity import Conn, PEADB, REDIS, start, stop, info_section, wait_for, encode

PEADB = os.environ.get('PEADB_CLAIM_BIN', PEADB)
AOF = ('--appendonly', 'yes', '--appendfsync', 'always', '--aof-use-rdb-preamble', 'no')


def ports():
    sockets = [socket.socket() for _ in range(2)]
    try:
        for s in sockets:
            s.bind(('127.0.0.1', 0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


def fields(row):
    return dict(zip(row[::2], row[1::2]))


def state(c, key='s'):
    group = fields(fields(c('XINFO', 'STREAM', key, 'FULL', 'COUNT', 0))['groups'][0])
    return group['last-delivered-id'], group['pending']


def seed(c, key='s'):
    for i in range(1, 4):
        assert c('XADD', key, f'{i}-0', 'f', f'v{i}') == f'{i}-0'
    assert c('XGROUP', 'CREATE', key, 'g', '0') == '+OK'


def aof_ready(c):
    wait_for(lambda: info_section(c, 'persistence')['aof_rewrite_in_progress'] == '0'
             and info_section(c, 'persistence')['aof_rewrite_scheduled'] == '0', 'AOF ready')


def aof_bytes(directory):
    root = pathlib.Path(directory)
    manifest = root / 'appendonlydir' / 'appendonly.aof.manifest'
    if not manifest.exists():
        return (root / 'appendonly.aof').read_bytes()
    files = [line.split()[1] for line in manifest.read_text().splitlines()
             if line.startswith('file ')]
    return b''.join((manifest.parent / f).read_bytes() for f in files)


def commands(payload):
    f = io.BytesIO(payload)
    out = []
    while line := f.readline():
        if line.startswith(b'#'):
            continue
        assert line.startswith(b'*'), line
        args = []
        for _ in range(int(line[1:])):
            length = f.readline()
            assert length.startswith(b'$'), length
            args.append(f.read(int(length[1:])).decode())
            assert f.read(2) == b'\r\n'
        out.append(args)
    return out


def local(binary):
    p, _ = ports()
    with tempfile.TemporaryDirectory(prefix='peadb-claim-options-') as td:
        proc = start(binary, p, td, AOF)
        c = None
        try:
            c = Conn(p)
            aof_ready(c)
            seed(c)
            assert c('XREADGROUP', 'GROUP', 'g', 'alice', 'STREAMS', 's', '>')
            assert c('XCLAIM', 's', 'g', 'bob', 0, '1-0', 'TIME', 1234,
                     'RETRYCOUNT', 7, 'JUSTID') == ['1-0']
            assert state(c)[1][0] == ['1-0', 'bob', 1234, 7], state(c)
            for raw_id in ('+1-0', ' 1-0', '1-+0', '1-00'):
                assert c('XCLAIM', 's', 'g', 'bob', 0, raw_id, 'TIME', 1234,
                         'RETRYCOUNT', 7, 'JUSTID') == ['1-0']
                assert state(c)[1][0] == ['1-0', 'bob', 1234, 7]
            for option in ('IDLE', 'TIME', 'RETRYCOUNT'):
                before = state(c)
                assert c('XCLAIM', 's', 'g', 'bad', 0, '2-0', option, 'bad') == (
                    f'-ERR Invalid {option} option argument for XCLAIM')
                assert state(c) == before
            for value in ('+1', ' 1', '01', '-0', '1 ', '9223372036854775808', ''):
                before = state(c)
                for option in ('IDLE', 'TIME', 'RETRYCOUNT'):
                    assert c('XCLAIM', 's', 'g', 'bad', 0, '2-0', option, value) == (
                        f'-ERR Invalid {option} option argument for XCLAIM')
                assert c('XCLAIM', 's', 'g', 'bad', value, '2-0') == (
                    '-ERR Invalid min-idle-time argument for XCLAIM')
                assert c('XAUTOCLAIM', 's', 'g', 'bad', value, '0') == (
                    '-ERR Invalid min-idle-time argument for XAUTOCLAIM')
                assert c('XAUTOCLAIM', 's', 'g', 'bad', 0, '0', 'COUNT', value) == (
                    '-ERR COUNT must be > 0')
                assert state(c) == before
            assert c('XCLAIM', 's', 'g', 'bad', 0, '2-0', 'LASTID', 'bad') == (
                '-ERR Invalid stream ID specified as stream command argument')
            before = state(c)
            assert c('XCLAIM', 's', 'g', 'bad', 0, '2-0', 'unknown') == (
                "-ERR Unrecognized XCLAIM option 'unknown'")
            assert state(c) == before
            assert c('XCLAIM', 's', 'g', 'bob', -1, '1-0', 'JUSTID') == ['1-0']
            assert state(c)[1][0][3] == 7
            assert c('XCLAIM', 's', 'g', 'bob', 0, '1-0', '1-0')
            assert state(c)[1][0][3] == 9
            assert c('XCLAIM', 's', 'g', 'bob', 0, '1-0', 'IDLE', 5000,
                     'RETRYCOUNT', 11, 'JUSTID') == ['1-0']
            assert 4900 <= c('XPENDING', 's', 'g', '-', '+', 10)[0][2] < 10000
            assert state(c)[1][0][3] == 11
            assert c('XCLAIM', 's', 'g', 'bob', 0, '1-0', 'IDLE', 0, 'TIME', 0,
                     'RETRYCOUNT', 0, 'JUSTID') == ['1-0']
            assert state(c)[1][0] == ['1-0', 'bob', 0, 0]
            assert c('XCLAIM', 's', 'g', 'bob', 0, '1-0', 'TIME', 1234, 'IDLE', -1,
                     'RETRYCOUNT', -2, 'JUSTID') == ['1-0']
            assert state(c)[1][0][2] > 1234 and state(c)[1][0][3] == 0
            assert c('XCLAIM', 's', 'g', 'bob', 0, '1-0', 'TIME', 9223372036854775807,
                     'RETRYCOUNT', -2) == [['1-0', ['f', 'v1']]]
            assert state(c)[1][0][2] <= int(time.time() * 1000)
            assert state(c)[1][0][3] == 1
            assert c('XAUTOCLAIM', 's', 'g', 'carol', -1, '(1-0', 'COUNT', 1,
                     'JUSTID')[1] == ['2-0']
            assert c('XAUTOCLAIM', 's', 'g', 'carol', 0, '(+1-0', 'COUNT', 1,
                     'JUSTID')[1] == ['2-0']
            assert c('XAUTOCLAIM', 's', 'g', 'carol', 0, 'bad') == (
                '-ERR Invalid stream ID specified as stream command argument')
            assert c('XAUTOCLAIM', 'missing', 'g', 'carol', 0, 'bad', 'COUNT', 0) == (
                '-ERR Invalid stream ID specified as stream command argument')
            assert c('XAUTOCLAIM', 's', 'g', 'carol', 0,
                     '(18446744073709551615-18446744073709551615') == (
                '-ERR invalid start ID for the interval')
            assert c('XCLAIM', 'missing', 'g', 'c', 'bad', 'bad').startswith('-NOGROUP ')
            assert c('XCLAIM', 's', 'g', 'empty', 0, 'LASTID', '+9-0') == []
            assert state(c)[0] == '9-0'
            expected = state(c)
            payload = aof_bytes(td)
            stream_commands = [a for a in commands(payload) if a[0].upper() == 'XCLAIM']
            assert stream_commands and all(len(a) == 14 and a[4] == '0'
                                           and a[6].upper() == 'TIME'
                                           and a[8].upper() == 'RETRYCOUNT'
                                           for a in stream_commands), stream_commands
            # Restart after wall-clock time advances: delivery time/count must remain exact.
            c.close(); c = None
            proc.kill(); proc.wait(timeout=10)
            time.sleep(0.05)
            proc = start(binary, p, td, AOF)
            c = Conn(p)
            assert state(c) == expected, (state(c), expected)
            assert c('BGREWRITEAOF').startswith('+Background append only file rewriting')
            aof_ready(c)
            c.close(); c = None
            proc.kill(); proc.wait(timeout=10)
            proc = start(binary, p, td, AOF)
            c = Conn(p)
            assert state(c) == expected, 'rewritten AOF lost PEL metadata'
            print('PASS claim options/AOF:', binary, flush=True)
        finally:
            if c:
                c.close()
            stop(proc)


def replicated(master_binary, replica_binary):
    p, q = ports()
    with tempfile.TemporaryDirectory(prefix='peadb-claim-replication-') as td:
        master = start(master_binary, p, td + '/m', AOF)
        replica = start(replica_binary, q, td + '/r')
        m = r = None
        try:
            m, r = Conn(p), Conn(q)
            aof_ready(m)
            assert m('CONFIG', 'SET', 'repl-diskless-sync', 'no') == '+OK'
            assert r('REPLICAOF', '127.0.0.1', p) == '+OK'
            wait_for(lambda: info_section(r, 'replication')['master_link_status'] == 'up', 'sync')
            # Streams are created after the empty snapshot: cross-engine stream RDBs are delta 5.
            assert m('SELECT', 5) == r('SELECT', 5) == '+OK'
            seed(m)
            assert m('WAIT', 1, 5000) == 1
            assert r('CLIENT', 'PAUSE', 300, 'WRITE') == '+OK'
            assert m('XREADGROUP', 'GROUP', 'g', 'alice', 'STREAMS', 's', '>')
            time.sleep(0.06)
            assert m('XCLAIM', 's', 'g', 'bob', 30, '1-0', 'JUSTID') == ['1-0']
            assert m('SET', 'barrier', 1) == '+OK'
            assert m('WAIT', 1, 5000) == 1
            assert state(r) == state(m), (state(r), state(m))
            assert m('XCLAIM', 's', 'g', 'bob', 0, '+1-0', 'TIME', 1234,
                     'RETRYCOUNT', 17, 'JUSTID') == ['1-0']
            assert m('WAIT', 1, 5000) == 1
            assert state(r) == state(m), 'raw master claim ID was not normalized on the replica'
            assert m('XCLAIM', 's', 'g', 'bob', 0, '1-0', 'TIME', 1234,
                     'RETRYCOUNT', 17, 'JUSTID') == ['1-0']
            assert m('XDEL', 's', '2-0') == 1
            assert m('XAUTOCLAIM', 's', 'g', 'carol', 0, '-', 'COUNT', 2,
                     'JUSTID') == ['3-0', ['1-0'], ['2-0']]
            assert m('WAIT', 1, 5000) == 1
            assert state(r) == state(m), (state(r), state(m))
            assert m('MULTI') == '+OK'
            assert m('XCLAIM', 's', 'g', 'dave', 0, '1-0', 'TIME', 2345,
                     'RETRYCOUNT', 23, 'JUSTID') == '+QUEUED'
            assert m('XCLAIM', 's', 'g', 'dave', 0, '3-0', 'JUSTID') == '+QUEUED'
            assert m('EXEC') == [['1-0'], ['3-0']]
            assert m('EVAL', "return redis.call('XCLAIM',KEYS[1],'g','eve',0,'1-0',"
                     "'TIME',3456,'RETRYCOUNT',29,'JUSTID')", 1, 's') == ['1-0']
            assert m('XDEL', 's', '3-0') == 1
            assert m('XCLAIM', 's', 'g', 'empty', 999999, '3-0', 'LASTID', '9-0') == []
            assert m('XCLAIM', 's', 'g', 'empty', 0, 'LASTID', '10-0') == []
            assert m('WAIT', 1, 5000) == 1
            assert state(r) == state(m) == ('10-0', [['1-0', 'eve', 3456, 29]])
            # One newly-created NOACK consumer must not create/notify it again in another key.
            seed(m, 't')
            assert m('XGROUP', 'CREATECONSUMER', 's', 'g', 'noack') == 1
            before = len(commands(aof_bytes(td + '/m')))
            assert m('XREADGROUP', 'GROUP', 'g', 'noack', 'NOACK', 'STREAMS',
                     's', 't', '>', '>')
            assert m('WAIT', 1, 5000) == 1
            assert state(r, 't') == state(m, 't') == ('3-0', [])
            new = commands(aof_bytes(td + '/m'))[before:]
            creations = [a[2:] for a in new if a[0].upper() == 'XGROUP'
                         and a[1].upper() == 'CREATECONSUMER']
            assert creations == [['t', 'g', 'noack']], creations
            # Repeated names with mixed IDs must not let a history request hide a new read.
            for requests in (('>', '0'), ('0', '>')):
                assert m('XGROUP', 'SETID', 't', 'g', '0') == '+OK'
                assert m('XREADGROUP', 'GROUP', 'g', 'mixed', 'NOACK', 'STREAMS',
                         't', 't', *requests)
                assert m('WAIT', 1, 5000) == 1
                assert state(r, 't') == state(m, 't') == ('3-0', [])
            assert m('XGROUP', 'SETID', 't', 'g', '0') == '+OK'
            before = len(commands(aof_bytes(td + '/m')))
            assert m('XREADGROUP', 'GROUP', 'g', 'mixed', 'NOACK', 'COUNT', 1,
                     'STREAMS', 't', 't', '>', '>') == [
                         ['t', [['1-0', ['f', 'v1']]]], ['t', [['2-0', ['f', 'v2']]]]]
            assert m('WAIT', 1, 5000) == 1
            assert state(r, 't') == state(m, 't') == ('2-0', [])
            updates = [a[4] for a in commands(aof_bytes(td + '/m'))[before:]
                       if a[:4] == ['XGROUP', 'SETID', 't', 'g']]
            assert updates == ['1-0', '2-0'], updates
            seed(m, 'u')
            before = state(m, 'u')
            assert m('XREADGROUP', 'GROUP', 'g', 'bad', 'STREAMS', 'u', 'missing',
                     '>', '>').startswith('-NOGROUP ')
            assert state(m, 'u') == before, 'failed multi-stream read changed the first PEL'
            assert 'bad' not in [fields(row)['name'] for row in m('XINFO', 'CONSUMERS', 'u', 'g')]
            # Rewinding the group reassigns existing PEL ownership without double-counting it.
            assert m('XGROUP', 'SETID', 'u', 'g', '0') == '+OK'
            assert m('XREADGROUP', 'GROUP', 'g', 'first', 'STREAMS', 'u', '>')
            assert m('XGROUP', 'SETID', 'u', 'g', '0') == '+OK'
            assert m('XREADGROUP', 'GROUP', 'g', 'second', 'STREAMS', 'u', '>')
            assert m('WAIT', 1, 5000) == 1
            assert state(r, 'u') == state(m, 'u')
            for conn in (m, r):
                counts = {fields(row)['name']: fields(row)['pending']
                          for row in conn('XINFO', 'CONSUMERS', 'u', 'g')}
                assert counts == {'first': 0, 'second': 3}, counts
            # Blocking wakeups use the same absolute-time claim effects.
            assert m('XGROUP', 'CREATE', 'blocked', 'g', '0', 'MKSTREAM') == '+OK'
            b = Conn(p)
            try:
                b('SELECT', 5)
                b.sock.sendall(encode('XREADGROUP', 'GROUP', 'g', 'woken', 'BLOCK', 3000,
                                     'STREAMS', 'blocked', '>'))
                wait_for(lambda: int(info_section(m, 'clients')['blocked_clients']) == 1,
                         'blocking group read')
                assert m('XADD', 'blocked', '1-0', 'f', 'v') == '1-0'
                assert b.read() == [['blocked', [['1-0', ['f', 'v']]]]]
            finally:
                b.close()
            assert m('SET', 'barrier', 2) == '+OK'
            assert m('WAIT', 1, 5000) == 1
            assert state(r, 'blocked') == state(m, 'blocked')
            before = state(r)
            assert m('EVAL', "redis.set_repl(redis.REPL_NONE); return redis.call('XCLAIM',"
                     "KEYS[1],'g','local',0,'1-0','TIME',4567,'RETRYCOUNT',31,'JUSTID')",
                     1, 's') == ['1-0']
            assert m('SET', 'barrier', 3) == '+OK'
            assert m('WAIT', 1, 5000) == 1
            assert state(r) == state(m) and state(r) != before
            assert state(m)[1] == [['1-0', 'local', 4567, 31]]
            raw = commands(aof_bytes(td + '/m'))
            assert not any(a[0].upper() in ('XAUTOCLAIM', 'XREADGROUP') for a in raw)
            assert not any(a[0].upper() == 'XCLAIM' and a[4] != '0' for a in raw)
            print('PASS exact delayed PEL replication:', master_binary, replica_binary, flush=True)
        finally:
            for c in (m, r):
                if c:
                    c.close()
            stop(replica); stop(master)


def renamed(binary):
    p, q = ports()
    extra = ('--rename-command', 'XREADGROUP', 'READGROUPX', '--rename-command', 'XCLAIM', 'CLAIM',
             '--rename-command', 'XAUTOCLAIM', 'AUTO')
    with tempfile.TemporaryDirectory(prefix='peadb-renamed-claims-') as td:
        master = start(binary, p, td + '/m', (*AOF, *extra))
        replica = start(binary, q, td + '/r')
        m = r = None
        try:
            m, r = Conn(p), Conn(q)
            aof_ready(m)
            m('CONFIG', 'SET', 'repl-diskless-sync', 'no')
            r('REPLICAOF', '127.0.0.1', p)
            wait_for(lambda: info_section(r, 'replication')['master_link_status'] == 'up', 'sync')
            seed(m)
            assert m('READGROUPX', 'GROUP', 'g', 'a', 'STREAMS', 's', '>') == [
                ['s', [[f'{i}-0', ['f', f'v{i}']] for i in range(1, 4)]]]
            assert m('CLAIM', 's', 'g', 'b', 0, '1-0', 'TIME', 1234,
                     'RETRYCOUNT', 7, 'JUSTID') == ['1-0']
            assert m('AUTO', 's', 'g', 'c', 0, '-', 'JUSTID')[1] == ['1-0', '2-0', '3-0']
            assert m('WAIT', 1, 5000) == 1
            assert state(r) == state(m)
            raw = commands(aof_bytes(td + '/m'))
            assert not any(a[0].upper() in ('READGROUPX', 'CLAIM', 'AUTO') for a in raw), raw
            print('PASS renamed stream effect propagation:', binary, flush=True)
        finally:
            for c in (m, r):
                if c:
                    c.close()
            stop(replica); stop(master)


def cross_aof(source_binary, target_binary):
    p, _ = ports()
    with tempfile.TemporaryDirectory(prefix='peadb-claim-cross-aof-') as td:
        proc = start(source_binary, p, td + '/source', AOF)
        c = None
        try:
            c = Conn(p)
            aof_ready(c)
            c('SELECT', 5)
            seed(c)
            assert c('XREADGROUP', 'GROUP', 'g', 'a', 'STREAMS', 's', '>')
            assert c('XCLAIM', 's', 'g', 'b', 0, '1-0', 'TIME', 1234,
                     'RETRYCOUNT', 31, 'JUSTID') == ['1-0']
            expected = state(c)
            payload = aof_bytes(td + '/source')
            c.close(); c = None
            proc.kill(); proc.wait(timeout=10)
            target = pathlib.Path(td, 'target')
            target.mkdir()
            (target / 'appendonly.aof').write_bytes(payload)
            proc = start(target_binary, p, str(target), AOF)
            c = Conn(p)
            c('SELECT', 5)
            assert state(c) == expected, (source_binary, target_binary, state(c), expected)
            print('PASS cross-engine stream AOF:', source_binary, target_binary, flush=True)
        finally:
            if c:
                c.close()
            stop(proc)


if __name__ == '__main__':
    assert REDIS, 'pinned Redis 7.2 reference required'
    local(REDIS)
    local(PEADB)
    if not os.environ.get('CLAIM_LOCAL_ONLY'):
        replicated(REDIS, REDIS)
        replicated(PEADB, PEADB)
        replicated(PEADB, REDIS)
        replicated(REDIS, PEADB)
        renamed(REDIS)
        renamed(PEADB)
        cross_aof(PEADB, REDIS)
        cross_aof(REDIS, PEADB)
