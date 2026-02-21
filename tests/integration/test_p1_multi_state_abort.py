#!/usr/bin/env python3
import pathlib, socket, subprocess, time

ROOT = pathlib.Path(__file__).resolve().parents[2]

def enc(*a):
    d=f"*{len(a)}\r\n".encode()
    for x in a:
        b=x.encode(); d+=f"${len(b)}\r\n".encode()+b+b"\r\n"
    return d

def rx(s,n):
    b=b""
    while len(b)<n:
        c=s.recv(n-len(b))
        if not c: raise RuntimeError("closed")
        b+=c
    return b

def rl(s):
    b=b""
    while not b.endswith(b"\r\n"): b+=rx(s,1)
    return b[:-2]

def recv(s):
    p=rx(s,1)
    if p==b'+': return rl(s).decode()
    if p==b'-': return ("ERR", rl(s).decode())
    if p==b':': return int(rl(s))
    if p==b'$':
        n=int(rl(s))
        if n==-1: return None
        d=rx(s,n); rx(s,2); return d.decode()
    if p==b'*':
        n=int(rl(s)); return [recv(s) for _ in range(n)]
    raise RuntimeError(p)

def cmd(s,*a):
    s.sendall(enc(*a)); return recv(s)

def main():
    p=subprocess.Popen([str(ROOT/"peadb-server"),"--port","6496","--bind","127.0.0.1","--loglevel","error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1",6496),timeout=3) as s1, \
             socket.create_connection(("127.0.0.1",6496),timeout=3) as s2:
            assert cmd(s1,"FLUSHALL")=="OK"
            assert cmd(s1,"SET","xx","1")=="OK"

            assert cmd(s1,"MULTI")=="OK"
            assert cmd(s1,"INCR","xx")=="QUEUED"
            assert cmd(s2,"CONFIG","SET","min-replicas-to-write","2")=="OK"
            e=cmd(s1,"EXEC")
            assert isinstance(e,tuple) and e[0]=="ERR" and "EXECABORT" in e[1] and "NOREPLICAS" in e[1]
            assert cmd(s1,"GET","xx")=="1"
            assert cmd(s2,"CONFIG","SET","min-replicas-to-write","0")=="OK"

            assert cmd(s2,"CONFIG","SET","replica-serve-stale-data","no")=="OK"
            assert cmd(s1,"MULTI")=="OK"
            assert cmd(s1,"GET","xx")=="QUEUED"
            assert cmd(s2,"REPLICAOF","localhsot","0")=="OK"
            e=cmd(s1,"EXEC")
            assert isinstance(e,tuple) and e[0]=="ERR" and "EXECABORT" in e[1] and "MASTERDOWN" in e[1]
            assert cmd(s2,"REPLICAOF","NO","ONE")=="OK"
            assert cmd(s2,"CONFIG","SET","replica-serve-stale-data","yes")=="OK"

        print("P1 multi state abort tests passed")
        return 0
    finally:
        p.terminate(); p.wait(timeout=3)

if __name__=="__main__":
    raise SystemExit(main())
