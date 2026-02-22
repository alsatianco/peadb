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
        if n==-1:return None
        d=rx(s,n); rx(s,2); return d.decode()
    if p==b'*':
        n=int(rl(s))
        if n==-1:return None
        return [recv(s) for _ in range(n)]
    raise RuntimeError(p)

def cmd(s,*a):
    s.sendall(enc(*a)); return recv(s)

def repl_cmd(s):
    if rx(s,1)!=b'*': raise RuntimeError('bad repl')
    n=int(rl(s)); out=[]
    for _ in range(n):
        assert rx(s,1)==b'$'
        ln=int(rl(s)); out.append(rx(s,ln).decode()); rx(s,2)
    return out

def read_n_repl(s,n):
    out=[]
    s.settimeout(2)
    for _ in range(n):
        out.append(repl_cmd(s))
    return out

def main():
    p=subprocess.Popen([str(ROOT/"peadb-server"),"--port","6514","--bind","127.0.0.1","--loglevel","error"])
    try:
        time.sleep(0.25)
        with socket.create_connection(("127.0.0.1",6514),timeout=2) as c, socket.create_connection(("127.0.0.1",6514),timeout=2) as r:
            assert cmd(c,"FLUSHALL")=="OK"
            r.sendall(b"SYNC\r\n")
            assert rx(r,1)==b'$'; assert rl(r)==b"0"

            assert cmd(c,"MSET","a{t}","1","b{t}","2","c{t}","3","d{t}","4")=="OK"
            assert cmd(c,"EVAL","return redis.call('mget', 'a{t}', 'b{t}', 'c{t}', 'd{t}')","0")==["1","2","3","4"]
            assert cmd(c,"SET","trailingkey","2")=="OK"
            ev = read_n_repl(r,3)
            ev = [e for e in ev if e and e[0].lower() != "select"]
            assert len(ev) == 2
            assert ev[0][0].lower()=="mset"
            assert ev[1][0].lower()=="set"
            assert all(e[0].lower()!="eval" for e in ev)

            assert cmd(c,"EVAL","redis.call('hmget', KEYS[1], 1, 2, 3)","1","key") is None
            assert cmd(c,"EVAL","redis.call('incrbyfloat', KEYS[1], 1)","1","key") is None
            assert cmd(c,"EVAL","redis.call('set', KEYS[1], '1', 'KEEPTTL')","1","key") is None
            ev2 = read_n_repl(r,2)
            assert ev2[0][0].lower()=="set" and ev2[0][1]=="key" and ev2[0][2]=="1" and ev2[0][3].lower()=="keepttl"
            assert ev2[1][0].lower()=="set" and ev2[1][1]=="key" and ev2[1][2]=="1" and ev2[1][3].lower()=="keepttl"

        print("P2 script replication shape tests passed")
        return 0
    finally:
        p.terminate(); p.wait(timeout=3)

if __name__=="__main__":
    raise SystemExit(main())
