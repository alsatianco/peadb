#!/usr/bin/env python3
import pathlib,socket,subprocess,time
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
 raise RuntimeError(p)

def cmd(s,*a):
 d=f"*{len(a)}\r\n".encode()
 for x in a:
  b=x.encode(); d+=f"${len(b)}\r\n".encode()+b+b"\r\n"
 s.sendall(d); return recv(s)

def main():
 # SETSLOT now resolves node tokens against a real node table (see
 # docs/decisions/p5-cluster-addressing-and-version-provenance.md), so redirection needs an
 # actual peer to point at rather than the old fictitious "remote" token, and the whole
 # CLUSTER command is gated on --cluster-enabled (cluster.c:5888-5891).
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6428','--bind','127.0.0.1',
   '--cluster-enabled','yes','--cluster-announce-ip','127.0.0.1','--loglevel','error'])
 peer=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6567','--bind','127.0.0.1',
   '--cluster-enabled','yes','--cluster-announce-ip','127.0.0.1','--loglevel','error'])
 try:
  wait_until_ready(6428)
  wait_until_ready(6567)
  with socket.create_connection(('127.0.0.1',6428),timeout=2) as s:
   assert cmd(s,'CLUSTER','MEET','127.0.0.1','6567')=='OK'
   slot=cmd(s,'CLUSTER','KEYSLOT','foo')
   assert cmd(s,'CLUSTER','SETSLOT',str(slot),'NODE','127.0.0.1:6567')=='OK'
   r=cmd(s,'GET','foo')
   assert r[0]=='ERR' and r[1]=='MOVED %d 127.0.0.1:6567' % slot, r
   assert cmd(s,'CLUSTER','SETSLOT',str(slot),'MIGRATING','127.0.0.1:6567')=='OK'
   r2=cmd(s,'GET','foo')
   assert r2[0]=='ERR' and r2[1]=='ASK %d 127.0.0.1:6567' % slot, r2
   assert cmd(s,'CLUSTER','SETSLOT',str(slot),'NODE','self')=='OK'
   # An unresolvable node is refused rather than silently accepted (Redis's wording).
   r3=cmd(s,'CLUSTER','SETSLOT',str(slot),'NODE','nosuchnode')
   assert r3==('ERR','ERR Unknown node nosuchnode'), r3
  print('M7 redirection tests passed')
  return 0
 finally:
  peer.terminate(); peer.wait(timeout=3)
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
