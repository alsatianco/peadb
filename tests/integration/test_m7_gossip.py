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
 p1=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6429','--bind','127.0.0.1','--loglevel','error'])
 p2=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6430','--bind','127.0.0.1','--loglevel','error'])
 try:
  time.sleep(0.3)
  with socket.create_connection(('127.0.0.1',6429),timeout=2) as a, socket.create_connection(('127.0.0.1',6430),timeout=2) as b:
   assert cmd(a,'CLUSTER','MEET','127.0.0.1','6430')=='OK'
   deadline=time.time()+2
   seen=False
   while time.time()<deadline:
    n1=cmd(a,'CLUSTER','NODES')
    n2=cmd(b,'CLUSTER','NODES')
    if '127.0.0.1:6430' in n1 and '127.0.0.1:6429' in n2:
      seen=True; break
    time.sleep(0.1)
   assert seen
  print('M7 gossip tests passed')
  return 0
 finally:
  p2.terminate(); p2.wait(timeout=3)
  p1.terminate(); p1.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
