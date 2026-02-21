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

def is_err_with(v,needle):
    return isinstance(v,tuple) and v[0]=="ERR" and needle in v[1]

def main():
    p=subprocess.Popen([str(ROOT/"peadb-server"),"--port","6513","--bind","127.0.0.1","--loglevel","error"])
    try:
        time.sleep(0.25)
        with socket.create_connection(("127.0.0.1",6513),timeout=2) as s:
            assert is_err_with(cmd(s,"EVAL","#!not-lua\nreturn 1","0"),"Unexpected engine in script shebang")
            assert cmd(s,"EVAL","#!lua\nreturn 1","0")==1
            assert is_err_with(cmd(s,"EVAL","#!lua badger=data\nreturn 1","0"),"Unknown lua shebang option")
            assert is_err_with(cmd(s,"EVAL","#!lua flags=allow-oom,what?\nreturn 1","0"),"Unexpected flag in script shebang")

            assert cmd(s,"SET","x","123")=="OK"
            assert cmd(s,"CONFIG","SET","maxmemory","1")=="OK"

            assert is_err_with(cmd(s,"EVAL","redis.call('set','x',1)\nreturn 1","1","x"),"OOM command not allowed")
            assert cmd(s,"EVAL","return redis.call('get','x')","1","x")=="123"
            assert is_err_with(cmd(s,"EVAL","#!lua\nreturn 1","0"),"OOM command not allowed")

            assert is_err_with(cmd(s,"EVAL","#!lua flags=\nreturn 1","0"),"OOM command not allowed")

            assert cmd(s,"EVAL","#!lua flags=allow-oom\nredis.call('set','x',1)\nreturn 1","1","x")==1
            assert cmd(s,"EVAL","#!lua flags=no-writes\nredis.call('get','x')\nreturn 1","0")==1
            assert cmd(s,"EVAL_RO","#!lua flags=no-writes\nredis.call('get','x')\nreturn 1","1","x")==1
            assert cmd(s,"EVAL","redis.call('get','x')\nreturn 1","1","x")==1
            assert cmd(s,"EVAL_RO","redis.call('get','x')\nreturn 1","1","x")==1

            assert cmd(s,"CONFIG","SET","maxmemory","0")=="OK"

        print("P2 shebang/oom tests passed")
        return 0
    finally:
        p.terminate(); p.wait(timeout=3)

if __name__=="__main__":
    raise SystemExit(main())
