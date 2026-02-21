#!/usr/bin/env python3
import pathlib, socket, subprocess, tempfile, time

ROOT = pathlib.Path(__file__).resolve().parents[2]


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


def start(cfg_path):
    p = subprocess.Popen([str(ROOT / 'peadb-server'), '--config', str(cfg_path)])
    time.sleep(0.25)
    return p


def main():
    with tempfile.TemporaryDirectory(prefix='peadb-m5-') as td:
        tdp = pathlib.Path(td)
        cfg = tdp / 'peadb.conf'
        cfg.write_text('\n'.join([
            'bind 127.0.0.1',
            'port 6409',
            f'dir {tdp}',
            'dbfilename dump.rdb',
            'loglevel error',
        ]) + '\n', encoding='utf-8')

        p = start(cfg)
        try:
            with socket.create_connection(('127.0.0.1', 6409), timeout=2) as s:
                assert cmd(s, 'FLUSHALL') == 'OK'
                assert cmd(s, 'SET', 'k', 'v') == 'OK'
                assert cmd(s, 'HSET', 'h', 'f', 'x') == 1
                assert cmd(s, 'LPUSH', 'l', 'a', 'b') == 2
                assert cmd(s, 'SADD', 'ss', 'm1', 'm2') == 2
                assert cmd(s, 'ZADD', 'z', '1', 'a') == 1
                assert cmd(s, 'SAVE') == 'OK'
                info = cmd(s, 'INFO', 'persistence')
                assert 'rdb_last_save_time:' in info
        finally:
            p.terminate(); p.wait(timeout=3)

        assert (tdp / 'dump.rdb').exists()

        p2 = start(cfg)
        try:
            with socket.create_connection(('127.0.0.1', 6409), timeout=2) as s2:
                assert cmd(s2, 'GET', 'k') == 'v'
                assert cmd(s2, 'HGET', 'h', 'f') == 'x'
                assert cmd(s2, 'LLEN', 'l') == 2
                assert cmd(s2, 'SCARD', 'ss') == 2
                assert cmd(s2, 'ZRANGE', 'z', '0', '-1') == ['a']
        finally:
            p2.terminate(); p2.wait(timeout=3)

    print('M5 snapshot roundtrip tests passed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
