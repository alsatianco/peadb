#!/usr/bin/env python3
import pathlib,socket,subprocess,sys,tempfile
ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready

def rx(s,n):
 b=b''
 while len(b)<n:
  c=s.recv(n-len(b))
  if not c: raise RuntimeError('closed')
  b+=c
 return b

def rl(s):
 b=b''
 while not b.endswith(b'\r\n'):
  b+=rx(s,1)
 return b[:-2]

def recv(s):
 p=rx(s,1)
 if p==b'+': return rl(s).decode()
 if p==b'-': return ('ERR',rl(s).decode())
 if p==b':': return int(rl(s))
 if p==b'$':
  n=int(rl(s));
  if n==-1:return None
  v=rx(s,n); rx(s,2); return v.decode()
 if p==b'*':
  n=int(rl(s));
  if n==-1:return None
  return [recv(s) for _ in range(n)]
 raise RuntimeError(p)

def cmd(s,*a):
 d=f"*{len(a)}\r\n".encode()
 for x in a:
  b=x.encode(); d+=f"${len(b)}\r\n".encode()+b+b"\r\n"
 s.sendall(d); return recv(s)

def main():
 # ignore_cleanup_errors: BGSAVE/BGREWRITEAOF fork children can briefly write
 # dump.rdb(.tmp)/appendonly.aof into td after the parent is terminated, racing the
 # temp-dir removal ("Directory not empty"). The product behaviour is correct; only
 # the test teardown needs to tolerate the race.
 with tempfile.TemporaryDirectory(prefix='peadb-m5cmd-', ignore_cleanup_errors=True) as td:
  cfg=pathlib.Path(td)/'p.conf'
  cfg.write_text('\n'.join([
   'bind 127.0.0.1',
   'port 6410',
   f'dir {td}',
   'dbfilename dump.rdb',
   'loglevel warning',
  ])+'\n',encoding='utf-8')
  p=subprocess.Popen([str(ROOT/'peadb-server'),'--config',str(cfg)])
  try:
   wait_until_ready(6410, process=p)
   with socket.create_connection(('127.0.0.1',6410),timeout=2) as s:
    assert cmd(s,'SET','a','1')=='OK'
    assert cmd(s,'SAVE')=='OK'
    assert cmd(s,'BGSAVE')=='Background saving started'
    # Like Redis, one child at a time: while the BGSAVE child still runs the rewrite is scheduled.
    assert cmd(s,'BGREWRITEAOF') in ('Background append only file rewriting started',
                                     'Background append only file rewriting scheduled')
    info=cmd(s,'INFO','persistence')
    assert 'rdb_last_save_time:' in info
  finally:
   p.terminate(); p.wait(timeout=3)
 print('M5 persistence command tests passed')
 return 0

if __name__=='__main__':
 raise SystemExit(main())
