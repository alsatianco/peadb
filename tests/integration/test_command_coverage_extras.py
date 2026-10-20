#!/usr/bin/env python3
import pathlib,socket,subprocess,sys
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
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6417','--bind','127.0.0.1','--loglevel','warning'])
 try:
  wait_until_ready(6417, process=p)
  with socket.create_connection(('127.0.0.1',6417),timeout=2) as s:
   # With no requirepass configured, AUTH is an error (matches Redis); the
   # connection remains usable because no authentication is required.
   r=cmd(s,'AUTH','x'); assert isinstance(r,tuple) and r[0]=='ERR' and 'any password' in r[1], r
   assert cmd(s,'SET','k','v')=='OK'
   assert cmd(s,'GETDEL','k')=='v'
   assert cmd(s,'SET','k2','v2')=='OK'
   assert cmd(s,'GETEX','k2','PERSIST')=='v2'
   assert cmd(s,'SET','k3','v3')=='OK'
   assert cmd(s,'EXPIREAT','k3','9999999999')==1
   assert cmd(s,'PEXPIREAT','k3','9999999999000')==1
   assert cmd(s,'PEXPIRE','k3','1000')==1
   assert cmd(s,'DEL','k3')==1
   assert cmd(s,'UNLINK','k3')==0
   assert cmd(s,'FUNCTION','FLUSH')=='OK'
  print('extra coverage tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
