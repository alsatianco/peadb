#!/usr/bin/env python3
import pathlib, socket, subprocess, threading, time

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
        n=int(rl(s)); return [recv(s) for _ in range(n)]
    raise RuntimeError(p)

def cmd(s,*a):
    s.sendall(enc(*a)); return recv(s)

def main():
    p=subprocess.Popen([str(ROOT/"peadb-server"),"--port","6497","--bind","127.0.0.1","--loglevel","error"])
    try:
        time.sleep(0.2)
        with socket.create_connection(("127.0.0.1",6497),timeout=3) as a, \
             socket.create_connection(("127.0.0.1",6497),timeout=3) as b:
            assert cmd(a,"FLUSHALL")=="OK"
            assert cmd(a,"CONFIG","SET","lua-time-limit","10")=="OK"
            assert cmd(a,"SET","xx","1")=="OK"

            done = {"reply": None}
            def run_eval():
                done["reply"] = cmd(a,"EVAL","while true do end","0")
            t=threading.Thread(target=run_eval,daemon=True)
            t.start()
            time.sleep(0.15)

            m=cmd(b,"MULTI")
            assert m=="OK" or (isinstance(m,tuple) and m[0]=="ERR" and "BUSY" in m[1])
            e=cmd(b,"INCR","xx")
            assert isinstance(e,tuple) and e[0]=="ERR" and ("BUSY" in e[1] or "MULTI" in e[1])
            e=cmd(b,"EXEC")
            if m=="OK":
                assert isinstance(e,tuple) and e[0]=="ERR" and "EXECABORT" in e[1] and "BUSY" in e[1]
            else:
                assert isinstance(e,tuple) and e[0]=="ERR" and "BUSY" in e[1]

            assert cmd(b,"SCRIPT","KILL")=="OK"
            t.join(timeout=2)
            assert done["reply"] is not None
            assert cmd(b,"PING")=="PONG"

        print("P1 multi busy execabort tests passed")
        return 0
    finally:
        p.terminate(); p.wait(timeout=3)

if __name__=="__main__":
    raise SystemExit(main())
