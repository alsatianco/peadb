"""Owned Redis-first direct load fixtures; no whole snapshot is created by the product."""
from _replication_snapshot import (MARK, Master, PEADB, REPLID, check, encode, info,
                                   reference, seed, server, verify, wait_for, wait_link)
from test_rdb_redis_interop import Conn

OLD_FUNCTION = ("#!lua name=old\nlocal n=0;redis.register_function{function_name='old',"
                "flags={'no-writes'},callback=function() n=n+1;return n end}")
LOADING = '-LOADING Redis is loading the dataset in memory'


def snapshot():
    with server(reference()) as (c, _, td, _):
        deadline = seed(c)
        for index in range(64):
            assert c('SET', f'padding:{index}', 'p' * 512) == '+OK'
        assert c('RPUSH', 'L', 'from-snapshot') == 1
        assert c('ZADD', 'Z', 1, 'member') == 1
        assert c('XADD', 'events', '1-0', 'field', 'value') == b'1-0'
        assert c('XGROUP', 'CREATE', 'events', 'g', '0') == '+OK'
        assert c('XREADGROUP', 'GROUP', 'g', 'consumer', 'STREAMS', 'events', '>')
        assert c('XDEL', 'events', '1-0') == 1  # preserve PEL of deleted entry
        assert c('XADD', 'events', '2-0', 'field', 'next') == b'2-0'
        stream = c('XINFO', 'STREAM', 'events', 'FULL')
        assert c('SAVE') == '+OK'
        return (td / 'dump.rdb').read_bytes(), deadline, stream


def seed_old(c):
    assert c('SET', 'old', 'old-value') == '+OK'
    assert c('FUNCTION', 'LOAD', OLD_FUNCTION) == b'old'
    assert c('SELECT', 15) == '+OK'
    assert c('SET', 'local', 'ttl') == '+OK'
    assert c('PEXPIRE', 'local', 60000) == 1
    deadline = c('PEXPIRETIME', 'local')
    assert c('SELECT', 0) == '+OK'
    assert c('XGROUP', 'CREATE', 'gone', 'old-group', '0', 'MKSTREAM') == '+OK'
    return deadline


def header(sock, replid, rdb, length, offset=0):
    frame = f'${len(rdb)}'.encode() if length else b'$EOF:' + MARK
    sock.sendall(b'\n\r\n+FULLRESYNC ' + replid.encode() + b' ' + str(offset).encode() + b'\r\n\n\r\n' + frame + b'\r\n')


def loading(c, matching):
    # INFO and old GET share a single pump opportunity; publication cannot split observation.
    c.sock.sendall(encode('INFO', 'persistence') + encode('GET', 'old'))
    state, old = c.read(), c.read()
    if matching:
        if b'async_loading:1\r\n' not in state:
            return False
        assert b'loading:0\r\n' in state and old == b'old-value', (state, old)
    else:
        if b'loading:1\r\n' not in state:
            return False
        assert b'async_loading:0\r\n' in state and old == LOADING, (state, old)
    return True


def verify_snapshot(c, deadline, stream):
    verify(c, deadline)
    assert c('XINFO', 'STREAM', 'events', 'FULL') == stream
    assert c('GET', 'old') is None
    assert c('SELECT', 15) == '+OK'
    assert c('GET', 'local') is None
    assert c('SELECT', 0) == '+OK'
    assert c('FCALL_RO', 'old', 0) == '-ERR Function not found'
