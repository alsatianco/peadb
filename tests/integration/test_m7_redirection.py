#!/usr/bin/env python3
import pathlib,socket,subprocess,time
ROOT=pathlib.Path(__file__).resolve().parents[2]

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
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6428','--bind','127.0.0.1','--loglevel','error'])
 try:
  time.sleep(0.2)
  with socket.create_connection(('127.0.0.1',6428),timeout=2) as s:
   slot=cmd(s,'CLUSTER','KEYSLOT','foo')
   assert cmd(s,'CLUSTER','SETSLOT',str(slot),'NODE','remote')=='OK'
   r=cmd(s,'GET','foo')
   assert r[0]=='ERR' and r[1].startswith('MOVED')
   assert cmd(s,'CLUSTER','SETSLOT',str(slot),'MIGRATING','remote')=='OK'
   r2=cmd(s,'GET','foo')
   assert r2[0]=='ERR' and r2[1].startswith('ASK')
   assert cmd(s,'CLUSTER','SETSLOT',str(slot),'NODE','self')=='OK'
  print('M7 redirection tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
