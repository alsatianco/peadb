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
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6406','--bind','127.0.0.1','--loglevel','error'])
 try:
  time.sleep(0.2)
  with socket.create_connection(('127.0.0.1',6406),timeout=2) as s:
   assert cmd(s,'FLUSHALL')=='OK'
   script1="redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
   assert cmd(s,'EVAL',script1,'1','k','v1')=='v1'
   assert cmd(s,'GET','k')=='v1'

   sha=cmd(s,'SCRIPT','LOAD',script1)
   assert cmd(s,'EVALSHA',sha,'1','k','v2')=='v2'
   assert cmd(s,'GET','k')=='v2'

   script2="redis.call('INCR', KEYS[1]); return redis.call('GET', KEYS[1])"
   assert cmd(s,'SET','ctr','0')=='OK'
   assert cmd(s,'EVAL',script2,'1','ctr')=='1'
   assert cmd(s,'EVAL',script2,'1','ctr')=='2'
   assert cmd(s,'GET','ctr')=='2'

  print('M4 lua2 atomic tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
