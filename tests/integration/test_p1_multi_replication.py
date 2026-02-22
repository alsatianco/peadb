#!/usr/bin/env python3
import pathlib
import socket
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]

def enc(*a):
    d = f"*{len(a)}\r\n".encode()
    for x in a:
        b = x.encode(); d += f"${len(b)}\r\n".encode() + b + b"\r\n"
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
        if n==-1:return None
        d=rx(s,n);rx(s,2);return d.decode()
    if p==b'*':
        n=int(rl(s));return [recv(s) for _ in range(n)]
    raise RuntimeError(p)

def cmd(s,*a):
    s.sendall(enc(*a)); return recv(s)

def repl_cmd(s):
    assert rx(s,1)==b'*'
    n=int(rl(s)); out=[]
    for _ in range(n):
        assert rx(s,1)==b'$'
        ln=int(rl(s)); out.append(rx(s,ln).decode()); rx(s,2)
    return out

def main():
    p=subprocess.Popen([str(ROOT/"peadb-server"),"--port","6487","--bind","127.0.0.1","--loglevel","error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1",6487),timeout=2) as c, socket.create_connection(("127.0.0.1",6487),timeout=2) as r:
            assert cmd(c,"FLUSHALL")=="OK"
            r.sendall(b"SYNC\r\n")
            assert rx(r,1)==b'$'; assert rl(r)==b"0"

            assert cmd(c,"MULTI")=="OK"
            assert cmd(c,"SET","a","1")=="QUEUED"
            assert cmd(c,"SET","b","2")=="QUEUED"
            ex=cmd(c,"EXEC")
            assert isinstance(ex,list) and len(ex)==2

            e1=repl_cmd(r)
            if e1[0].lower()=="select":
                e1=repl_cmd(r)
            assert e1[0].lower()=="multi"
            e2=repl_cmd(r)
            if e2[0].lower()=="select":
                e2=repl_cmd(r)
            assert e2[0].lower()=="set"
            e3=repl_cmd(r)
            if e3[0].lower()=="select":
                e3=repl_cmd(r)
            assert e3[0].lower()=="set"
            assert repl_cmd(r)[0].lower()=="exec"

            assert cmd(c,"MULTI")=="OK"
            assert cmd(c,"DEL","nope")=="QUEUED"
            ex=cmd(c,"EXEC")
            assert isinstance(ex,list) and ex[0]==0
            assert cmd(c,"INCR","x")==1
            # no DEL from no-op transaction, only INCR
            ev=repl_cmd(r)
            if ev[0].lower()=="select":
                ev=repl_cmd(r)
            assert ev[0].lower()=="incr"

        print("P1 multi replication tests passed")
        return 0
    finally:
        p.terminate(); p.wait(timeout=3)

if __name__=="__main__":
    raise SystemExit(main())
