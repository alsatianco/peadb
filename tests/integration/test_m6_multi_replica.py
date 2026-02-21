#!/usr/bin/env python3
import pathlib,subprocess,sys,tempfile,time
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import skip_if_no_redis_server, skip_if_no_redis_cli
ROOT=pathlib.Path(__file__).resolve().parents[2]
REDIS_SERVER=skip_if_no_redis_server()
REDIS_CLI=skip_if_no_redis_cli()

def rc(port,*args):
 return subprocess.check_output([REDIS_CLI,'-h','127.0.0.1','-p',str(port),*args],text=True).strip()

def start_redis(port,d):
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
 with tempfile.TemporaryDirectory(prefix='peadb-m6mr-') as td:
  m=start_redis(6423,td)
  r1=start_peadb(6424)
  r2=start_peadb(6425)
  try:
   rc(6423,'SET','mk','mv')
   rc(6423,'HSET','h','f','x')
   rc(6424,'REPLICAOF','127.0.0.1','6423')
   rc(6425,'REPLICAOF','127.0.0.1','6423')
   deadline=time.time()+4
   ok=False
   while time.time()<deadline:
    if rc(6424,'GET','mk')=='mv' and rc(6425,'GET','mk')=='mv':
      ok=True; break
    time.sleep(0.1)
   assert ok
  finally:
   r2.terminate(); r2.wait(timeout=3)
   r1.terminate(); r1.wait(timeout=3)
   m.terminate(); m.wait(timeout=3)
 print('M6 multi-replica tests passed')
 return 0

if __name__=='__main__':
 raise SystemExit(main())
