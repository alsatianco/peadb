#!/usr/bin/env python3
"""Command discovery must survive formatting and cover the running server's table."""
import pathlib
import socket
import sys
import tempfile

from test_replica_offset_parity import Conn, PEADB, start, stop

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts' / 'ci'))
import check_command_test_coverage as coverage
import generate_compat_scoreboard as scoreboard


def main():
    with tempfile.TemporaryDirectory(prefix='peadb-command-discovery-') as td:
        fixture = pathlib.Path(td, 'commands.cpp')
        fixture.write_text('''
            t.emplace("SET", CommandSpec{});
            t.emplace(
                "AUTH", CommandSpec{});
            t . emplace ( "RESTORE-ASKING", CommandSpec{} );
        ''')
        expected = ['AUTH', 'RESTORE-ASKING', 'SET']
        assert scoreboard.extract_commands(fixture) == expected
        assert coverage.extract_commands(fixture.read_text()) == expected
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        proc = start(PEADB, port, td + '/server')
        c = None
        try:
            c = Conn(port)
            live = sorted(name.upper() for name in c('COMMAND', 'LIST') if '|' not in name)
            source = ROOT / 'src' / 'command.cpp'
            assert scoreboard.extract_commands(source) == live
            assert coverage.extract_commands(source.read_text()) == live
            assert len(live) == c('COMMAND', 'COUNT')
            print(f'PASS command discovery: {len(live)} live registrations')
        finally:
            if c:
                c.close()
            stop(proc)


if __name__ == '__main__':
    main()
