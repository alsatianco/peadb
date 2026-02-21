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
  n=int(rl(s))
  if n==-1:return None
  v=rx(s,n); rx(s,2); return v.decode()
 if p==b'*':
  n=int(rl(s))
  if n==-1:return None
  return [recv(s) for _ in range(n)]
 raise RuntimeError(p)

def cmd(s,*a):
 d=f"*{len(a)}\r\n".encode()
 for x in a:
  b=x.encode(); d+=f"${len(b)}\r\n".encode()+b+b"\r\n"
 s.sendall(d); return recv(s)

def main():
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6404','--bind','127.0.0.1','--loglevel','error'])
 try:
  time.sleep(0.2)
  with socket.create_connection(('127.0.0.1',6404),timeout=2) as s:
   assert cmd(s,'FLUSHALL')=='OK'
   assert cmd(s,'DISCARD')[0]=='ERR'
   assert cmd(s,'MULTI')=='OK'
   assert cmd(s,'SET','a','1')=='QUEUED'
   assert cmd(s,'INCR','a')=='QUEUED'
   out=cmd(s,'EXEC')
   assert out==['OK',2]
   assert cmd(s,'GET','a')=='2'

  with socket.create_connection(('127.0.0.1',6404),timeout=2) as s1, socket.create_connection(('127.0.0.1',6404),timeout=2) as s2:
   assert cmd(s1,'WATCH','w')=='OK'
   assert cmd(s2,'SET','w','x')=='OK'
   assert cmd(s1,'MULTI')=='OK'
   assert cmd(s1,'GET','w')=='QUEUED'
   assert cmd(s1,'EXEC') is None
   assert cmd(s1,'UNWATCH')=='OK'

  print('M4 TX tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
