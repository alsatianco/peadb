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
  if n==-1: return None
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
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6402','--bind','127.0.0.1','--loglevel','error'])
 try:
  time.sleep(0.2)
  with socket.create_connection(('127.0.0.1',6402),timeout=2) as s:
   assert cmd(s,'FLUSHALL')=='OK'
   assert cmd(s,'SADD','s','a','b','a')==2
   assert cmd(s,'SCARD','s')==2
   assert cmd(s,'SISMEMBER','s','a')==1
   members=cmd(s,'SMEMBERS','s')
   assert sorted(members)==['a','b']
   scan=cmd(s,'SSCAN','s','0','COUNT','10')
   assert isinstance(scan,list) and len(scan)==2 and scan[0]=='0'
   assert sorted(scan[1])==['a','b']
   assert cmd(s,'SREM','s','a')==1
   assert cmd(s,'SISMEMBER','s','a')==0
   assert cmd(s,'TYPE','s')=='set'
   assert cmd(s,'OBJECT','ENCODING','s')=='listpack'
   assert cmd(s,'SET','x','1')=='OK'
   e=cmd(s,'SADD','x','m')
   assert e[0]=='ERR' and 'WRONGTYPE' in e[1]
  print('M4 set tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
