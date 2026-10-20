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
 raise RuntimeError(p)

def cmd(s,*a):
 d=f"*{len(a)}\r\n".encode()
 for x in a:
  b=x.encode(); d+=f"${len(b)}\r\n".encode()+b+b"\r\n"
 s.sendall(d); return recv(s)

def main():
 # CLUSTER MEET is gated on --cluster-enabled at the top of the handler, before any
 # subcommand dispatch (cluster.c:5888-5891), so both nodes need cluster mode on.
 port_a, port_b = 6429, 6430
 announce_a, announce_b = '127.0.0.1', '127.0.0.1'
 p1=subprocess.Popen([str(ROOT/'peadb-server'),'--port',str(port_a),'--bind','127.0.0.1',
   '--cluster-enabled','yes','--cluster-announce-ip',announce_a,'--loglevel','warning'])
 p2=subprocess.Popen([str(ROOT/'peadb-server'),'--port',str(port_b),'--bind','127.0.0.1',
   '--cluster-enabled','yes','--cluster-announce-ip',announce_b,'--loglevel','warning'])
 try:
  wait_until_ready(port_a, process=p1)
  wait_until_ready(port_b, process=p2)
  with socket.create_connection(('127.0.0.1',port_a),timeout=2) as a, \
       socket.create_connection(('127.0.0.1',port_b),timeout=2) as b:
   assert cmd(a,'CLUSTER','MEET',announce_b,str(port_b))=='OK'
   # CLUSTER MEET's peer registration (cluster_register_peer) and its synchronous MEETBACK
   # round trip (try_send_cluster_meetback) both complete before MEET's own +OK reply, so
   # each side should already know about the other node's announced address immediately —
   # the poll loop below is a defensive margin, not the expected path. On a genuine failure
   # this must report what CLUSTER NODES actually said, not silently time out.
   expect_b_in_a = announce_b + ':' + str(port_b)
   expect_a_in_b = announce_a + ':' + str(port_a)
   deadline=time.time()+2
   n1=n2=''
   while time.time()<deadline:
    n1=cmd(a,'CLUSTER','NODES')
    n2=cmd(b,'CLUSTER','NODES')
    if expect_b_in_a in n1 and expect_a_in_b in n2:
      break
    time.sleep(0.1)
   assert expect_b_in_a in n1, ('node A never learned about B', expect_b_in_a, n1)
   assert expect_a_in_b in n2, ('node B never learned about A (no MEETBACK)', expect_a_in_b, n2)
  print('M7 gossip tests passed')
  return 0
 finally:
  p2.terminate(); p2.wait(timeout=3)
  p1.terminate(); p1.wait(timeout=3)

if __name__=='__main__':
 raise SystemExit(main())
