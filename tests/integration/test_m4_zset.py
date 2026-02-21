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
 if p==b'*':
  n=int(rl(s)); return [recv(s) for _ in range(n)]
 raise RuntimeError(p)

def cmd(s,*a):
 d=f"*{len(a)}\r\n".encode()
 for x in a:
  b=x.encode(); d+=f"${len(b)}\r\n".encode()+b+b"\r\n"
 s.sendall(d); return recv(s)

def main():
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6403','--bind','127.0.0.1','--loglevel','error'])
 try:
  time.sleep(0.2)
  with socket.create_connection(('127.0.0.1',6403),timeout=2) as s:
   assert cmd(s,'FLUSHALL')=='OK'
   assert cmd(s,'ZADD','z','1','a','2','b')==2
   assert cmd(s,'ZADD','z','CH','2','b','3','c')==1
   assert cmd(s,'ZRANGE','z','0','-1')==['a','b','c']
   zws=cmd(s,'ZRANGE','z','0','-1','WITHSCORES')
   assert zws==['a','1','b','2','c','3']
   assert cmd(s,'ZADD','z','INCR','1.5','a')=='2.5'
   assert cmd(s,'ZRANGE','z','0','0','WITHSCORES')==['b','2']
   zs=cmd(s,'ZSCAN','z','0','COUNT','10')
   assert isinstance(zs,list) and len(zs)==2 and zs[0]=='0'
   pmin=cmd(s,'ZPOPMIN','z')
   assert pmin==['b','2']
   pmax=cmd(s,'ZPOPMAX','z')
   assert pmax==['c','3']
   assert cmd(s,'TYPE','z')=='zset'
   assert cmd(s,'OBJECT','ENCODING','z')=='listpack'
   assert cmd(s,'SET','x','1')=='OK'
   e=cmd(s,'ZADD','x','1','m')
   assert e[0]=='ERR' and 'WRONGTYPE' in e[1]
  print('M4 zset tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
