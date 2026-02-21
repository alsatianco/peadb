#!/usr/bin/env python3
import pathlib, socket, subprocess, tempfile, time
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import skip_if_no_redis_server, skip_if_no_redis_cli
ROOT = pathlib.Path(__file__).resolve().parents[2]
REDIS_SERVER = skip_if_no_redis_server()
REDIS_CLI = skip_if_no_redis_cli()


def cmd(host, port, *args):
    return subprocess.check_output([REDIS_CLI,'-h',host,'-p',str(port),*args], text=True).strip()


def start_peadb(port):
    p = subprocess.Popen([str(ROOT/'peadb-server'),'--port',str(port),'--bind','127.0.0.1','--loglevel','error'])
    time.sleep(0.25)
    return p


def start_redis_from_dir(port, d):
    conf = pathlib.Path(d)/'redis.conf'
    conf.write_text('\n'.join([
        'bind 127.0.0.1',
        f'port {port}',
        f'dir {d}',
        'dbfilename dump.rdb',
        'appendonly no',
        'save ""',
        'daemonize no',
    ])+'\n', encoding='utf-8')
    p = subprocess.Popen([REDIS_SERVER, str(conf)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.25)
    return p


def main():
    with tempfile.TemporaryDirectory(prefix='peadb-rdb-') as td:
        # Build a redis RDB and import into peadb.
        r = start_redis_from_dir(6412, td)
        try:
            cmd('127.0.0.1', 6412, 'SET', 'k', 'v')
            cmd('127.0.0.1', 6412, 'HSET', 'h', 'f', 'x')
            cmd('127.0.0.1', 6412, 'SAVE')
        finally:
            r.terminate(); r.wait(timeout=3)

        p = start_peadb(6413)
        try:
            subprocess.check_call(['python3', str(ROOT/'scripts/redis/import_rdb_via_redis.py'), '--rdb', f'{td}/dump.rdb', '--peadb-port', '6413'])
            assert cmd('127.0.0.1', 6413, 'GET', 'k') == 'v'
            assert cmd('127.0.0.1', 6413, 'HGET', 'h', 'f') == 'x'

            out_rdb = pathlib.Path(td)/'out.rdb'
            subprocess.check_call(['python3', str(ROOT/'scripts/redis/export_rdb_via_redis.py'), '--peadb-port', '6413', '--out', str(out_rdb)])
            assert out_rdb.exists() and out_rdb.stat().st_size > 0
        finally:
            p.terminate(); p.wait(timeout=3)

    print('M5 RDB bridge tests passed')
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
