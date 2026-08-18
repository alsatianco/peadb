#!/usr/bin/env python3
"""INFO must emit a real "# Cluster" section and return an empty bulk for unknown sections,
matching Redis's genRedisInfoString (~/.cache/peadb/redis-7.2.5/src/server.c)."""
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
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6561','--bind','127.0.0.1','--loglevel','error'])
 try:
  wait_until_ready(6561)
  with socket.create_connection(('127.0.0.1',6561),timeout=5) as s:
   # Redis emits a real "# Cluster" section; PeaDB is standalone here so cluster_enabled:0.
   c=cmd(s,'INFO','cluster')
   assert c.startswith('# Cluster\r\n'), f"INFO cluster should emit its own section: {c!r}"
   assert 'cluster_enabled:0\r\n' in c, f"INFO cluster must report cluster_enabled: {c!r}"
   assert 'redis_version' not in c, f"INFO cluster must not fall back to # Server: {c!r}"
   # INFO all must include the Cluster section.
   assert '# Cluster\r\n' in cmd(s,'INFO','all'), 'INFO all is missing # Cluster'
   # An unrecognised section returns an empty string, matching server.c genRedisInfoString.
   assert cmd(s,'INFO','nosuchsection')=='', 'unknown INFO section must return an empty bulk'
  print('P5 INFO section tests passed')
 finally:
  p.terminate(); p.wait(timeout=10)

main()
