#!/usr/bin/env python3
import pathlib,socket,subprocess,time
ROOT=pathlib.Path(__file__).resolve().parents[2]
import sys
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

def check_cluster_surface(port):
 with socket.create_connection(('127.0.0.1',port),timeout=2) as s:
  info=cmd(s,'CLUSTER','INFO')
  assert 'cluster_state:fail' in info and 'cluster_slots_assigned:0' in info, info
  nodes=cmd(s,'CLUSTER','NODES')
  assert 'myself,master' in nodes, nodes
  slots=cmd(s,'CLUSTER','SLOTS')
  assert slots==[], slots
  shards=cmd(s,'CLUSTER','SHARDS')
  assert shards==[], shards
  # KEYSLOT works once cluster mode is on
  ks=cmd(s,'CLUSTER','KEYSLOT','foo'); assert isinstance(ks,int)

def check_standalone_disabled(port):
 # cluster-enabled is a startup-only (IMMUTABLE_CONFIG) gate, checked at the very top of
 # clusterCommand() before any subcommand dispatch (cluster.c:5887-5891) — a standalone node
 # must refuse every CLUSTER subcommand with the same error, including read-only ones like
 # KEYSLOT and HELP, not just the mutating ones.
 with socket.create_connection(('127.0.0.1',port),timeout=2) as s:
  for args in (('CLUSTER','INFO'), ('CLUSTER','NODES'), ('CLUSTER','SLOTS'),
               ('CLUSTER','SHARDS'), ('CLUSTER','MYID'), ('CLUSTER','KEYSLOT','foo'),
               ('CLUSTER','HELP'), ('CLUSTER','ADDSLOTS','0'), ('CLUSTER','MEET','127.0.0.1','1')):
   reply=cmd(s,*args)
   assert reply==('ERR','ERR This instance has cluster support disabled'), (args, reply)

def main():
 # CLUSTER is gated on --cluster-enabled at the top of the handler, before any subcommand
 # dispatch (cluster.c:5888-5891) — including KEYSLOT — so this needs cluster mode on. With
 # no slots assigned yet, cluster_state is genuinely "fail" (not "ok"): a freshly
 # cluster-enabled, unconfigured node owns nothing, matching real Redis's own
 # not-all-slots-covered state rather than the old fabricated "always ok once active".
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6427','--bind','127.0.0.1',
   '--cluster-enabled','yes','--cluster-announce-ip','127.0.0.1','--loglevel','warning'])
 p_standalone=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6428','--bind','127.0.0.1',
   '--loglevel','warning'])
 try:
  wait_until_ready(6427, process=p)
  wait_until_ready(6428, process=p_standalone)
  check_cluster_surface(6427)
  check_standalone_disabled(6428)
  print('M7 cluster surface tests passed')
  return 0
 finally:
  p.terminate(); p.wait(timeout=3)
  p_standalone.terminate(); p_standalone.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
