#!/usr/bin/env python3
import pathlib,socket,subprocess,time
ROOT=pathlib.Path(__file__).resolve().parents[2]
PEADB_BIN = ROOT/'build/peadb-server' if (ROOT/'build/peadb-server').exists() else ROOT/'peadb-server'

def free_port():
 s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
 s.bind(('127.0.0.1',0))
 p=s.getsockname()[1]
 s.close()
 return p

def wait_ready(port,p,secs=2.0):
 end=time.time()+secs
 while time.time()<end:
  if p.poll() is not None:
   raise RuntimeError('peadb-server exited before becoming ready')
  try:
   with socket.create_connection(('127.0.0.1',port),timeout=0.1):
    return
  except OSError:
   time.sleep(0.02)
 raise RuntimeError('peadb-server did not become ready in time')

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
 port=free_port()
 p=subprocess.Popen([str(PEADB_BIN),'--port',str(port),'--bind','127.0.0.1','--loglevel','error'])
 try:
  wait_ready(port,p)
  with socket.create_connection(('127.0.0.1',port),timeout=2) as s:
   assert cmd(s,'FLUSHALL')=='OK'
   assert cmd(s,'SCRIPT','FLUSH')=='OK'
   sha=cmd(s,'SCRIPT','LOAD',"return ARGV[1]")
   assert isinstance(sha,str) and len(sha)==40
   assert cmd(s,'SCRIPT','EXISTS',sha,'deadbeef'*5)==[1,0]
   assert cmd(s,'EVAL',"return 'ok'",'0')=='ok'
   assert cmd(s,'EVAL',"return KEYS[1]",'1','k1')=='k1'
   assert cmd(s,'EVALSHA',sha,'0','v1')=='v1'
   nos=cmd(s,'EVALSHA','f'*40,'0')
   assert nos[0]=='ERR' and 'NOSCRIPT' in nos[1]
   nk=cmd(s,'EVAL','return 1','2','k')
   assert nk[0]=='ERR'
   assert cmd(s,'SCRIPT','KILL')[0]=='ERR'
  print('M4 lua tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
