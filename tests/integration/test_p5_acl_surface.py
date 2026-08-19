#!/usr/bin/env python3
"""ACL SETUSER used to return +OK and throw the rule away. This pins the truthful surface:
introspection (WHOAMI/USERS/LIST/CAT/GETUSER/GENPASS/HELP) answers for real, and anything
that would create or change a rule (SETUSER/DELUSER/LOAD/SAVE/LOG/DRYRUN) refuses and names
the documented gap (compat/delta.md items 1 and 7), rather than affirming work it did not do."""
import pathlib, socket, subprocess, sys, time
ROOT = pathlib.Path(__file__).resolve().parents[2]

def wait_up(port, auth_required=False, timeout=5.0):
 # PING returns +PONG normally, or -NOAUTH once requirepass is set (PING isn't in the
 # no_auth allowlist), so a requirepass server needs the auth_required branch — mirrors
 # test_b1_security.py's wait_up; the shared _readiness.wait_until_ready only checks
 # +PONG and would spin to timeout against a requirepass server.
 deadline = time.time() + timeout
 while time.time() < deadline:
  try:
   with socket.create_connection(('127.0.0.1', port), timeout=0.5) as s:
    s.sendall(b'*1\r\n$4\r\nPING\r\n')
    data = s.recv(64)
    if data.startswith(b'+PONG') or (auth_required and data.startswith(b'-NOAUTH')):
     return
  except OSError:
   pass
  time.sleep(0.05)
 raise RuntimeError(f'peadb-server on 127.0.0.1:{port} did not come up within {timeout}s')

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

def check_nopass():
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6562','--bind','127.0.0.1','--loglevel','error'])
 try:
  wait_up(6562)
  with socket.create_connection(('127.0.0.1',6562),timeout=5) as s:
   assert cmd(s,'ACL','WHOAMI')=='default'
   assert cmd(s,'ACL','USERS')==['default']
   assert cmd(s,'ACL','LIST')==['user default on nopass sanitize-payload ~* &* +@all']
   cats=cmd(s,'ACL','CAT')
   assert cats[0]=='keyspace' and 'scripting' in cats and len(cats)==21, cats
   assert 'GET' in [c.upper() for c in cmd(s,'ACL','CAT','string')]
   assert cmd(s,'ACL','CAT','nosuchcat')==('ERR',"ERR Unknown category 'nosuchcat'")
   g=cmd(s,'ACL','GETUSER','default')
   assert g[0]=='flags' and g[1]==['on','nopass','sanitize-payload'], g
   assert g[2]=='passwords' and g[3]==[], g
   assert g[4]=='commands' and g[5]=='+@all', g
   assert g[6]=='keys' and g[7]=='~*', g
   assert g[8]=='channels' and g[9]=='&*', g
   assert g[10]=='selectors' and g[11]==[], g
   assert cmd(s,'ACL','GETUSER','nobody') is None
   assert isinstance(cmd(s,'ACL','HELP'),list)
   # Rule-changing subcommands must refuse, and must name the documented gap.
   for sub in (['SETUSER','alice','on','>p'],['DELUSER','alice'],['LOAD'],['SAVE'],
               ['LOG'],['DRYRUN','default','GET','k']):
    r=cmd(s,'ACL',*sub)
    assert isinstance(r,tuple) and 'not supported' in r[1] and 'delta.md' in r[1], (sub,r)
   # GENPASS is truthful with no rule engine: 64 hex chars by default.
   gp=cmd(s,'ACL','GENPASS')
   assert len(gp)==64 and all(c in '0123456789abcdef' for c in gp), gp
   assert cmd(s,'ACL','GENPASS')!=gp, 'GENPASS must not be deterministic'
   # Unknown subcommand uses Redis's exact wording.
   assert cmd(s,'ACL','NOSUCHCMD')==('ERR',
     "ERR unknown subcommand or wrong number of arguments for 'NOSUCHCMD'. Try ACL HELP.")
  print('  nopass ok')
 finally:
  p.terminate(); p.wait(timeout=10)

def check_withpass():
 import hashlib
 p=subprocess.Popen([str(ROOT/'peadb-server'),'--port','6563','--bind','127.0.0.1',
                     '--requirepass','hunter2','--loglevel','error'])
 try:
  wait_up(6563, auth_required=True)
  with socket.create_connection(('127.0.0.1',6563),timeout=5) as s:
   assert cmd(s,'AUTH','hunter2')=='OK'
   h=hashlib.sha256(b'hunter2').hexdigest()
   assert cmd(s,'ACL','LIST')==[f'user default on sanitize-payload #{h} ~* &* +@all']
   g=cmd(s,'ACL','GETUSER','default')
   assert g[1]==['on','sanitize-payload'], g
   assert g[3]==[h], g
  print('  requirepass ok')
 finally:
  p.terminate(); p.wait(timeout=10)

check_nopass(); check_withpass()
print('P5 ACL surface tests passed')
