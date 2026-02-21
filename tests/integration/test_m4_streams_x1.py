#!/usr/bin/env python3
import pathlib,socket,subprocess,time,re
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

def is_stream_id(x):
 return isinstance(x,str) and re.match(r'^\d+-\d+$',x)

def main():
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6407','--bind','127.0.0.1','--loglevel','error'])
 try:
  time.sleep(0.2)
  with socket.create_connection(('127.0.0.1',6407),timeout=2) as s:
   assert cmd(s,'FLUSHALL')=='OK'
   id1=cmd(s,'XADD','st','*','f1','v1')
   assert is_stream_id(id1)
   id2=cmd(s,'XADD','st','*','f2','v2')
   assert is_stream_id(id2)
   assert cmd(s,'XLEN','st')==2

   rng=cmd(s,'XRANGE','st','-','+')
   assert isinstance(rng,list) and len(rng)==2
   assert rng[0][0]==id1 and rng[1][0]==id2

   rev=cmd(s,'XREVRANGE','st','+','-')
   assert isinstance(rev,list) and len(rev)==2
   assert rev[0][0]==id2 and rev[1][0]==id1

  print('M4 streams X1 tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
