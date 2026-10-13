#!/usr/bin/env python3
"""Named replication credentials, rejected AUTH and reconnects against Redis 7.2.5."""
import os
import pathlib
import socket
import subprocess
import tempfile

from _redis_path import redis_server_path
from test_replica_offset_parity import (Conn, PEADB, check, info, stop,
                                       wait_for, wait_link)
from test_replica_resync_parity import ports

REDIS = redis_server_path()  # Pinned 7.2.5 first; CI's installed Redis also runs the ACL smoke.


def launch(binary, port, directory, config=''):
    os.makedirs(directory, exist_ok=True)
    conf = pathlib.Path(directory, 'replica.conf')
    conf.write_text(config)
    log = pathlib.Path(directory, 'server.log')
    with log.open('wb') as output:
        proc = subprocess.Popen([binary, str(conf), '--port', str(port), '--bind', '127.0.0.1',
                                 '--dir', directory, '--save', '', '--loglevel', 'notice',
                                 '--shutdown-timeout', '0', '--repl-diskless-sync', 'no'],
                                stdout=output, stderr=subprocess.STDOUT)
    try:
        def ready():
            if proc.poll() is not None:
                raise AssertionError(f'{binary} exited: {log.read_text()}')
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.1):
                    return True
            except OSError:
                return False
        wait_for(ready, 'server start')
        return proc, log
    except BaseException:
        stop(proc)
        raise


def authenticated_sync(binary):
    mp, rp = ports(2)
    procs, conns = [], []
    with tempfile.TemporaryDirectory(prefix='peadb-auth-sync-') as td:
        try:
            proc, _ = launch(REDIS, mp, td + '/master'); procs.append(proc)
            m = Conn(mp); conns.append(m)
            # Only the commands needed by the replica; no access to application keys.
            check(m('ACL', 'SETUSER', 'repl-user', 'on', '>sync password',
                    '+ping', '+replconf', '+psync') == '+OK', 'replication ACL user')
            m('SET', 'initial', 'snapshot')
            m('SELECT', 5); m('SET', 'counter', '1'); m('SELECT', 0)
            check(m('ACL', 'SETUSER', 'default', 'off') == '+OK', 'disable default user')
            config = ('masteruser overridden\nmasteruser repl-user\n'
                      'masterauth "sync password"\n'
                      f'replicaof 127.0.0.1 {mp}\n')
            proc, log = launch(binary, rp, td + '/replica', config); procs.append(proc)
            r = Conn(rp); conns.append(r)
            check(r('CONFIG', 'GET', 'masteruser') == ['masteruser', 'repl-user'],
                  'startup masteruser, last value wins')
            check(r('CONFIG', 'GET', 'masterauth') == ['masterauth', 'sync password'],
                  'startup masterauth')
            wait_link(r)
            check(r('GET', 'initial') == 'snapshot', 'authenticated full snapshot')
            r('SELECT', 5); m('SELECT', 5)
            check(r('GET', 'counter') == '1', 'snapshot database 5')
            full = int(info(m, 'sync_full', 'stats'))
            for user, password in [('absent-user', 'sync password'), ('repl-user', 'incorrect')]:
                before = log.stat().st_size
                partial = int(info(m, 'sync_partial_ok', 'stats'))
                check(r('CONFIG', 'SET', 'masteruser', user, 'masterauth', password) == '+OK',
                      'runtime credential update')
                # Changing credentials leaves an already authenticated link running, like Redis.
                check(m('INCR', 'counter') >= 2 and m('WAIT', 1, 3000) == 1,
                      'credentials apply on the next handshake')
                check(m('CLIENT', 'KILL', 'TYPE', 'replica') == 1, 'disconnect authenticated link')
                expected = str(m('INCR', 'counter'))
                wait_for(lambda: 'Unable to AUTH to MASTER: -WRONGPASS' in
                         log.read_text()[before:], 'rejected authentication logged')
                check(info(r, 'master_link_status', 'replication') == 'down', 'bad AUTH stays down')
                check(m('WAIT', 1, 100) == 0, 'bad AUTH cannot acknowledge writes')
                check(r('GET', 'counter') != expected, 'no stream applied after bad AUTH')
                check(r('CONFIG', 'SET', 'masteruser', 'repl-user',
                        'masterauth', 'sync password') == '+OK', 'repair both credentials')
                wait_link(r)
                wait_for(lambda: r('GET', 'counter') == expected, 'cached stream catch-up')
                check(int(info(m, 'sync_partial_ok', 'stats')) > partial, 'authenticated PSYNC resume')
                check(int(info(m, 'sync_full', 'stats')) == full, 'no duplicate full load on reconnect')
            print('PASS ACL full sync/rejected credentials/partial reconnect:', binary, flush=True)
        finally:
            for c in conns: c.close()
            for proc in reversed(procs): stop(proc)


def read_command(stream):
    line = stream.readline()
    check(line.startswith(b'*'), 'master received RESP command')
    args = []
    for _ in range(int(line[1:])):
        n = int(stream.readline()[1:])
        args.append(stream.read(n))
        check(stream.read(2) == b'\r\n', 'command bulk terminator')
    return args


def default_user_sync(master_binary, replica_binary):
    mp, rp = ports(2)
    procs, conns = [], []
    with tempfile.TemporaryDirectory(prefix='peadb-auth-default-') as td:
        try:
            proc, _ = launch(master_binary, mp, td + '/master', 'requirepass "default password"\n')
            procs.append(proc)
            m = Conn(mp); conns.append(m)
            check(m('AUTH', 'default password') == '+OK', 'master password')
            m('SET', 'initial', 'default-user-snapshot')
            proc, _ = launch(replica_binary, rp, td + '/replica',
                             'masterauth "default password"\n'); procs.append(proc)
            r = Conn(rp); conns.append(r)
            for username in ('default', ''):
                r('REPLICAOF', 'NO', 'ONE')
                check(r('CONFIG', 'SET', 'masteruser', username) == '+OK', 'default user selection')
                r('REPLICAOF', '127.0.0.1', mp); wait_link(r)
                check(r('GET', 'initial') == 'default-user-snapshot', 'default-user full sync')
                m('SET', 'live', username or 'password-only')
                check(m('WAIT', 1, 3000) == 1, 'default-user live ACK')
                check(r('GET', 'live') == (username or 'password-only'), 'default-user stream')
            print('PASS named default/password-only sync:', master_binary, replica_binary, flush=True)
        finally:
            for c in conns: c.close()
            for proc in reversed(procs): stop(proc)


def handshake(binary):
    """A controlled master verifies AUTH arity/bytes and Redis's permissive replies."""
    with tempfile.TemporaryDirectory(prefix='peadb-auth-wire-') as td:
        rp = ports(1)[0]
        proc, _ = launch(binary, rp, td)
        r = Conn(rp)
        try:
            cases = [('', b'password', [b'AUTH', b'password']),
                     ('named', b'password', [b'AUTH', b'named', b'password']),
                     ('named', b'', None), ('', b'', None),
                     ('named\0tail', b'binary\0password',
                      [b'AUTH', b'named', b'binary\0password'])]
            for i, (user, password, expected) in enumerate(cases):
                with socket.socket() as listener:
                    listener.bind(('127.0.0.1', 0)); listener.listen(); listener.settimeout(10)
                    check(r('CONFIG', 'SET', 'masteruser', user, 'masterauth', password) == '+OK',
                          'runtime credentials accepted')
                    check(r('CONFIG', 'GET', 'masteruser') == ['masteruser', user.split('\0')[0]],
                          'masteruser follows Redis C-string semantics')
                    refused = r('CONFIG', 'SET', 'masteruser', 'discarded', 'maxclients', 0)
                    check(refused.startswith('-ERR CONFIG SET failed'), 'invalid batch refused')
                    check(r('CONFIG', 'GET', 'masteruser') == ['masteruser', user.split('\0')[0]],
                          'rejected batch preserves credentials')
                    r('REPLICAOF', '127.0.0.1', listener.getsockname()[1])
                    with listener.accept()[0] as peer:
                        peer.settimeout(10)
                        with peer.makefile('rb') as stream:
                            check(read_command(stream) == [b'PING'], 'pre-auth PING')
                            # Redis accepts any positive reply, NOAUTH, NOPERM and legacy errors.
                            peer.sendall([b'+ALIVE\r\n', b'-NOAUTH Authentication required.\r\n',
                                          b'-NOPERM denied\r\n', b'-ERR operation not permitted\r\n',
                                          b'+PONG\r\n'][i])
                            if expected:
                                check(read_command(stream) == expected, 'AUTH bytes/arity')
                                peer.sendall(b'+AUTHENTICATED\r\n')
                            cmd = read_command(stream)
                            check(cmd[:2] == [b'REPLCONF', b'listening-port'], 'port follows AUTH')
                            peer.sendall(b'-ERR legacy master has no listening-port\r\n')
                            check(read_command(stream)[:2] == [b'REPLCONF', b'capa'], 'capabilities')
                            peer.sendall(b'-ERR legacy master has no capa\r\n')
                            check(read_command(stream)[0] == b'PSYNC', 'optional REPLCONF errors tolerated')
                            peer.sendall(b'-ERR fixture complete\r\n')
                    r('REPLICAOF', 'NO', 'ONE')
            print('PASS AUTH wire forms/empty settings/binary password/handshake replies:',
                  binary, flush=True)
        finally:
            r.close(); stop(proc)


if __name__ == '__main__':
    check(REDIS is not None, 'this interop regression requires redis-server (7.2.5 preferred)')
    print('Replication AUTH reference:', subprocess.check_output([REDIS, '--version'], text=True).strip(),
          flush=True)
    for binary in (REDIS, os.environ.get('PEADB_REPLICA_BIN', PEADB)):
        authenticated_sync(binary)
        handshake(binary)
        default_user_sync(binary, binary)
    default_user_sync(REDIS, os.environ.get('PEADB_REPLICA_BIN', PEADB))
