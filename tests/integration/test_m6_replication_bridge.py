#!/usr/bin/env python3
import pathlib, subprocess, sys, tempfile, time
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import skip_if_no_redis_server, skip_if_no_redis_cli
ROOT = pathlib.Path(__file__).resolve().parents[2]
REDIS_SERVER = skip_if_no_redis_server()
REDIS_CLI = skip_if_no_redis_cli()

def rc(port,*args):
 return subprocess.check_output([REDIS_CLI,'-h','127.0.0.1','-p',str(port),*args],text=True).strip()

def start_redis(port, d):
 conf=pathlib.Path(d)/f'redis-{port}.conf'
 conf.write_text('\n'.join(['bind 127.0.0.1',f'port {port}',f'dir {d}','dbfilename dump.rdb','appendonly no','save ""','daemonize no'])+'\n',encoding='utf-8')
 p=subprocess.Popen([REDIS_SERVER,str(conf)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 time.sleep(0.25)
 return p

def start_peadb(port):
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port',str(port),'--bind','127.0.0.1','--loglevel','error'])
 time.sleep(0.25)
 return p

def main():
 with tempfile.TemporaryDirectory(prefix='peadb-repl-') as td:
  r=start_redis(6415,td)
  p=start_peadb(6416)
  try:
   rc(6415,'SET','k','v')
   rc(6415,'HSET','h','f','x')
   subprocess.check_call(['python3',str(ROOT/'scripts/redis/sync_from_redis.py'),'--source-port','6415','--target-port','6416'])
   assert rc(6416,'GET','k')=='v'
   assert rc(6416,'HGET','h','f')=='x'
  finally:
   p.terminate(); p.wait(timeout=3)
   r.terminate(); r.wait(timeout=3)
 print('M6 replication bridge tests passed')
 return 0

if __name__=='__main__':
 raise SystemExit(main())
