#!/usr/bin/env python3
import pathlib,socket,subprocess,time,re
ROOT=pathlib.Path(__file__).resolve().parents[2]

CMDS=[('SET','a','1'),('HSET','h','f','x'),('LPUSH','l','a','b'),('SADD','s','m1','m2'),('ZADD','z','1','a')]

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

def off(info):
 m=re.search(r'master_repl_offset:(\d+)',info)
 return int(m.group(1))

def run(port):
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port',str(port),'--bind','127.0.0.1','--loglevel','error'])
 try:
  time.sleep(0.2)
  with socket.create_connection(('127.0.0.1',port),timeout=2) as s:
   cmd(s,'FLUSHALL')
   for c in CMDS: cmd(s,*c)
   i=cmd(s,'INFO','replication')
   return off(i)
 finally:
  p.terminate(); p.wait(timeout=3)

def main():
 o1=run(6421)
 o2=run(6422)
 assert o1==o2
 print('M6 replication determinism tests passed')
 return 0

if __name__=='__main__':
 raise SystemExit(main())
