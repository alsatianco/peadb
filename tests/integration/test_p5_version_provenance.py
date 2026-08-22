#!/usr/bin/env python3
"""INFO's peadb_version must equal the repo-root VERSION file, and redis_version must stay 7.2.5."""
import pathlib,socket,subprocess,sys
ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _readiness import wait_until_ready

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
  if n==-1: return None
  v=rx(s,n); rx(s,2); return v.decode()
 if p==b'*':
  n=int(rl(s))
  if n==-1: return None
  return [recv(s) for _ in range(n)]
 raise RuntimeError(p)

def cmd(s,*a):
 d=f"*{len(a)}\r\n".encode()
 for x in a:
  b=x.encode(); d+=f"${len(b)}\r\n".encode()+b+b"\r\n"
 s.sendall(d); return recv(s)

def main():
 want=(ROOT/'VERSION').read_text().strip()
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6300','--bind','127.0.0.1','--loglevel','error'])
 try:
  wait_until_ready(6300)
  with socket.create_connection(('127.0.0.1',6300),timeout=5) as s:
   blob=cmd(s,'INFO','server')
   assert f"peadb_version:{want}\r\n" in blob, f"INFO peadb_version does not match VERSION ({want!r}): {blob!r}"
   assert "redis_version:7.2.5\r\n" in blob, "redis_version must stay the 7.2.5 compatibility target"
   # The default INFO must carry peadb_version too, not only INFO server.
   assert f"peadb_version:{want}\r\n" in cmd(s,'INFO'), "default INFO is missing peadb_version"
  print('P5 version provenance tests passed')
 finally:
  p.terminate(); p.wait(timeout=10)

main()
