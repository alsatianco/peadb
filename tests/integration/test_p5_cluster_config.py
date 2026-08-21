#!/usr/bin/env python3
"""P5 cluster configuration plumbing: cluster-enabled and cluster-announce-* config, CONFIG GET
bind reporting the real bind address instead of a hardcoded 0.0.0.0, redis_mode deriving from
cluster-enabled (server.c:5477), and the wildcard-announce startup refusal (see
docs/decisions/p5-cluster-addressing-and-version-provenance.md)."""
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

def case_config_surface():
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6564','--bind','127.0.0.1',
                     '--cluster-enabled','yes','--cluster-announce-ip','10.9.8.7',
                     '--cluster-announce-port','7100','--cluster-announce-bus-port','17100',
                     '--loglevel','error'])
 try:
  wait_until_ready(6564)
  with socket.create_connection(('127.0.0.1',6564),timeout=5) as s:
   def cget(k):
    r=cmd(s,'CONFIG','GET',k); return dict(zip(r[0::2],r[1::2])).get(k)
   assert cget('cluster-enabled')=='yes', cget('cluster-enabled')
   assert cget('cluster-announce-ip')=='10.9.8.7'
   assert cget('cluster-announce-port')=='7100'
   assert cget('cluster-announce-bus-port')=='17100'
   # CONFIG GET bind must report the real bind address, not a hardcoded 0.0.0.0.
   assert cget('bind')=='127.0.0.1', cget('bind')
   assert 'cluster_enabled:1' in cmd(s,'INFO','cluster')
   # redis_mode must derive from cluster-enabled (server.c:5477), not stay hardcoded standalone.
   info_server = cmd(s,'INFO','server')
   assert 'redis_mode:cluster\r\n' in info_server, info_server
   # The # Server cluster_enabled field must agree with # Cluster and redis_mode — same global.
   assert 'cluster_enabled:1\r\n' in info_server, info_server
   # The three announce options are runtime-modifiable in Redis; cluster-enabled is not.
   assert cmd(s,'CONFIG','SET','cluster-announce-ip','10.1.1.1')=='OK'
   assert cget('cluster-announce-ip')=='10.1.1.1'
   r = cmd(s,'CONFIG','SET','cluster-enabled','no')
   assert isinstance(r,tuple) and r[0]=='ERR', r
   assert "can't set immutable config" in r[1], r
  print('P5 cluster config: enabled + CONFIG GET/SET surface passed')
 finally:
  p.terminate(); p.wait(timeout=10)

def case_standalone_redis_mode():
 # No --cluster-enabled at all: redis_mode and cluster_enabled must both read as disabled,
 # matching pre-existing standalone behaviour (they must not drift apart from each other).
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6567','--bind','127.0.0.1','--loglevel','error'])
 try:
  wait_until_ready(6567)
  with socket.create_connection(('127.0.0.1',6567),timeout=5) as s:
   info_server = cmd(s,'INFO','server')
   assert 'redis_mode:standalone\r\n' in info_server, info_server
   assert 'cluster_enabled:0\r\n' in info_server, info_server
  print('P5 cluster config: standalone redis_mode passed')
 finally:
  p.terminate(); p.wait(timeout=10)

def case_wildcard_announce_refused():
 # CONTROLLER ADDENDUM 1: cluster-enabled yes + a wildcard bind + no explicit cluster-announce-ip
 # must refuse to start rather than announce an address (0.0.0.0) that no peer can dial.
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6565','--bind','0.0.0.0',
                     '--cluster-enabled','yes','--loglevel','error'],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
 try:
  try:
   rc = p.wait(timeout=10)
  except subprocess.TimeoutExpired:
   p.kill(); p.wait(timeout=5)
   raise AssertionError('server with --cluster-enabled yes --bind 0.0.0.0 and no '
                        '--cluster-announce-ip did not refuse to start within 10s')
  assert rc != 0, f'server must refuse to start (wildcard announce), got exit code {rc}'
  out = p.stdout.read().decode(errors='replace')
  assert 'cluster-announce-ip' in out, f'refusal message must name cluster-announce-ip: {out!r}'
 finally:
  if p.poll() is None:
   p.terminate(); p.wait(timeout=10)
 print('P5 cluster config: wildcard-announce refusal passed')

def case_explicit_wildcard_announce_ip_refused():
 # Final review finding: the startup guard used to gate on cluster_announce_ip.empty(), so an
 # EXPLICIT wildcard announce-ip (rather than one reached via the bind fallback) sailed through
 # and the server started advertising an address no peer can dial. The guard must check the
 # resolved announced host unconditionally.
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6570','--bind','127.0.0.1',
                     '--cluster-enabled','yes','--cluster-announce-ip','0.0.0.0',
                     '--loglevel','error'],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
 try:
  try:
   rc = p.wait(timeout=10)
  except subprocess.TimeoutExpired:
   p.kill(); p.wait(timeout=5)
   raise AssertionError('server with --cluster-announce-ip 0.0.0.0 did not refuse to start '
                        'within 10s')
  assert rc != 0, f'server must refuse an explicit wildcard announce-ip, got exit code {rc}'
  out = p.stdout.read().decode(errors='replace')
  assert 'cluster-announce-ip' in out, f'refusal message must name cluster-announce-ip: {out!r}'
 finally:
  if p.poll() is None:
   p.terminate(); p.wait(timeout=10)
 print('P5 cluster config: explicit wildcard cluster-announce-ip refused at startup passed')

def case_config_set_announce_ip_rejects_wildcard():
 # CONFIG SET cluster-announce-ip used to accept any string while the two port parameters
 # beside it were range-checked. Reject the same wildcard forms the startup guard refuses.
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6571','--bind','127.0.0.1',
                     '--cluster-enabled','yes','--loglevel','error'])
 try:
  wait_until_ready(6571)
  with socket.create_connection(('127.0.0.1',6571),timeout=5) as s:
   for wildcard in ('0.0.0.0', '::', '*'):
    r = cmd(s,'CONFIG','SET','cluster-announce-ip',wildcard)
    assert isinstance(r,tuple) and r[0]=='ERR', (wildcard, r)
    assert 'cluster-announce-ip' in r[1] and 'wildcard' in r[1], (wildcard, r)
   assert cmd(s,'CONFIG','SET','cluster-announce-ip','203.0.113.9')=='OK'
   assert _cget(s,'cluster-announce-ip')=='203.0.113.9'
   # Emptying it back out is still fine here: bind ('127.0.0.1') is not itself a wildcard, so
   # the fallback CONFIG SET would land on is safe. Only a wildcard fallback must be refused.
   assert cmd(s,'CONFIG','SET','cluster-announce-ip','')=='OK'
   assert _cget(s,'cluster-announce-ip')==''
  print('P5 cluster config: CONFIG SET cluster-announce-ip rejects wildcard values passed')
 finally:
  p.terminate(); p.wait(timeout=10)

def case_announce_port_zero_accepted_without_warning():
 # Review finding: 0 is Redis's own documented sentinel meaning "use the default" for
 # cluster-announce-port (redis-7.2.5/src/config.c:3171-3172; it is also the example value in
 # redis.conf:1807), not an invalid value. The startup path used to log "ignoring invalid
 # cluster-announce-port: 0" for it while still honouring the fallback correctly — telling an
 # operator their setting was ignored when it was not. It must accept 0 silently, the same way
 # CONFIG SET already does, and fall back to the client port.
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6572','--bind','127.0.0.1',
                     '--cluster-enabled','yes','--cluster-announce-ip','10.9.8.7',
                     '--cluster-announce-port','0','--loglevel','warning'],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
 try:
  wait_until_ready(6572)
  with socket.create_connection(('127.0.0.1',6572),timeout=5) as s:
   assert _cget(s,'cluster-announce-port')=='6572', _cget(s,'cluster-announce-port')
 finally:
  p.terminate(); p.wait(timeout=10)
 out = p.stdout.read().decode(errors='replace')
 assert 'ignoring invalid cluster-announce-port' not in out, out
 print('P5 cluster config: explicit cluster-announce-port 0 accepted without a bogus warning '
       'passed')

def case_config_set_announce_ip_empty_rejected_when_bind_is_wildcard():
 # Review finding: CONFIG SET cluster-announce-ip '' (empty) was accepted with no validation —
 # only literal wildcard strings were rejected. A node can only ever start with cluster-enabled
 # yes and a wildcard bind if it also has an explicit, non-wildcard cluster-announce-ip (the
 # startup guard refuses every other combination), so this override may be the only thing
 # standing between the announced host and that wildcard bind. Emptying it must be refused.
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6573','--bind','0.0.0.0',
                     '--cluster-enabled','yes','--cluster-announce-ip','203.0.113.5',
                     '--loglevel','error'])
 try:
  wait_until_ready(6573)
  with socket.create_connection(('127.0.0.1',6573),timeout=5) as s:
   r = cmd(s,'CONFIG','SET','cluster-announce-ip','')
   assert isinstance(r,tuple) and r[0]=='ERR', r
   assert 'cluster-announce-ip' in r[1] and 'wildcard' in r[1], r
   assert _cget(s,'cluster-announce-ip')=='203.0.113.5', _cget(s,'cluster-announce-ip')
  print('P5 cluster config: CONFIG SET cluster-announce-ip "" rejected when bind is a wildcard '
        'passed')
 finally:
  p.terminate(); p.wait(timeout=10)

def case_wildcard_bind_with_explicit_announce_ip_starts():
 # Same wildcard bind, but with cluster-announce-ip set explicitly: the hazard is covered by
 # the operator's own configuration, so the server must start normally.
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6566','--bind','0.0.0.0',
                     '--cluster-enabled','yes','--cluster-announce-ip','203.0.113.5',
                     '--loglevel','error'])
 try:
  wait_until_ready(6566)
  with socket.create_connection(('127.0.0.1',6566),timeout=5) as s:
   assert cmd(s,'PING')=='PONG'
   r=cmd(s,'CONFIG','GET','bind'); d=dict(zip(r[0::2],r[1::2]))
   assert d.get('bind')=='0.0.0.0', d
   r=cmd(s,'CONFIG','GET','cluster-announce-ip'); d=dict(zip(r[0::2],r[1::2]))
   assert d.get('cluster-announce-ip')=='203.0.113.5', d
  print('P5 cluster config: wildcard bind + explicit announce-ip starts normally passed')
 finally:
  p.terminate(); p.wait(timeout=10)

def _cget(s, k):
 r = cmd(s, 'CONFIG', 'GET', k)
 return dict(zip(r[0::2], r[1::2])).get(k)

def _self_nodes_line(s):
 nodes = cmd(s, 'CLUSTER', 'NODES')
 return [ln for ln in nodes.split('\n') if 'myself' in ln][0]

def case_announce_reregisters_after_cluster_touch():
 # Final review finding: CONFIG SET cluster-announce-* used to leave a stale advertised
 # address once any CLUSTER command had already lazily snapshotted the node table
 # (ensure_self_registered()) — CONFIG GET would show the new value while CLUSTER
 # NODES/SLOTS kept the old host/port/id. Order 1: touch CLUSTER first, then CONFIG SET.
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6568','--bind','127.0.0.1',
                     '--cluster-enabled','yes','--loglevel','error'])
 try:
  wait_until_ready(6568)
  with socket.create_connection(('127.0.0.1',6568),timeout=5) as s:
   old_id = cmd(s,'CLUSTER','MYID')  # forces the node-table snapshot
   assert cmd(s,'CONFIG','SET','cluster-announce-ip','203.0.113.9')=='OK'
   assert cmd(s,'CONFIG','SET','cluster-announce-port','7100')=='OK'
   assert cmd(s,'CONFIG','SET','cluster-announce-bus-port','17200')=='OK'
   assert _cget(s,'cluster-announce-ip')=='203.0.113.9'
   new_id = cmd(s,'CLUSTER','MYID')
   assert new_id != old_id, 'a re-announced address must mint a new id (decision record)'
   self_line = _self_nodes_line(s)
   assert self_line.startswith(new_id + ' 203.0.113.9:7100@17200 '), self_line
   slots = cmd(s,'CLUSTER','SLOTS')
   assert slots == [], slots  # no slots assigned in this test — just checking the address
  print('P5 cluster config: CONFIG SET announce-* re-registers after a CLUSTER touch passed')
 finally:
  p.terminate(); p.wait(timeout=10)

def case_announce_reregisters_before_cluster_touch():
 # Order 2: CONFIG SET before any CLUSTER command has ever run, so the node table doesn't
 # exist yet. The first CLUSTER call must build it from the already-updated settings, not
 # from whatever was live at startup.
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6569','--bind','127.0.0.1',
                     '--cluster-enabled','yes','--loglevel','error'])
 try:
  wait_until_ready(6569)
  with socket.create_connection(('127.0.0.1',6569),timeout=5) as s:
   assert cmd(s,'CONFIG','SET','cluster-announce-ip','203.0.113.9')=='OK'
   assert cmd(s,'CONFIG','SET','cluster-announce-port','7100')=='OK'
   assert cmd(s,'CONFIG','SET','cluster-announce-bus-port','17200')=='OK'
   myid = cmd(s,'CLUSTER','MYID')  # first-ever CLUSTER call
   self_line = _self_nodes_line(s)
   assert self_line.startswith(myid + ' 203.0.113.9:7100@17200 '), self_line
  print('P5 cluster config: CONFIG SET announce-* re-registers before a CLUSTER touch passed')
 finally:
  p.terminate(); p.wait(timeout=10)

def main():
 case_config_surface()
 case_standalone_redis_mode()
 case_wildcard_announce_refused()
 case_explicit_wildcard_announce_ip_refused()
 case_config_set_announce_ip_rejects_wildcard()
 case_announce_port_zero_accepted_without_warning()
 case_config_set_announce_ip_empty_rejected_when_bind_is_wildcard()
 case_wildcard_bind_with_explicit_announce_ip_starts()
 case_announce_reregisters_after_cluster_touch()
 case_announce_reregisters_before_cluster_touch()
 print('P5 cluster config tests passed')

main()
