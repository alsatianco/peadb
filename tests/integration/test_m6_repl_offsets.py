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

def get_off(info):
 m=re.search(r'master_repl_offset:(\d+)',info)
 return int(m.group(1)) if m else None

def main():
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6420','--bind','127.0.0.1','--loglevel','error'])
 try:
  time.sleep(0.2)
  with socket.create_connection(('127.0.0.1',6420),timeout=2) as s:
   info1=cmd(s,'INFO','replication')
   assert 'master_replid:' in info1
   o1=get_off(info1); assert isinstance(o1,int)
   assert cmd(s,'SET','a','1')=='OK'
   info2=cmd(s,'INFO','replication')
   o2=get_off(info2); assert isinstance(o2,int) and o2>o1
  print('M6 replication offset tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
