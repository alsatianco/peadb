#!/usr/bin/env python3
import pathlib,socket,subprocess,time,re
ROOT=pathlib.Path(__file__).resolve().parents[2]
import sys
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

def isid(x): return isinstance(x,str) and re.match(r'^\d+-\d+$',x)

def main():
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6408','--bind','127.0.0.1','--loglevel','warning'])
 try:
  wait_until_ready(6408, process=p)
  with socket.create_connection(('127.0.0.1',6408),timeout=2) as s:
   assert cmd(s,'FLUSHALL')=='OK'
   id1=cmd(s,'XADD','st','*','f','v1'); assert isid(id1)
   id2=cmd(s,'XADD','st','*','f','v2'); assert isid(id2)
   assert cmd(s,'XGROUP','CREATE','st','g','0')=='OK'

   # COUNT 1: XREADGROUP without COUNT reads ALL available entries (Redis semantics), so
   # bound it to one so acking id1 below empties the PEL.
   r1=cmd(s,'XREADGROUP','GROUP','g','c1','COUNT','1','STREAMS','st','>')
   assert isinstance(r1,list) and len(r1)==1
   got_id=r1[0][1][0][0]
   assert got_id==id1

   p1=cmd(s,'XPENDING','st','g')
   assert isinstance(p1,list) and p1[0]>=1

   assert cmd(s,'XACK','st','g',id1)==1
   p2=cmd(s,'XPENDING','st','g')
   assert isinstance(p2,list) and p2[0]==0

  print('M4 streams X2 tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
