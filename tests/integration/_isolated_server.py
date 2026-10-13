"""Own a scratch data directory for servers whose tests do not exercise restart.

Disk-backed replication writes dbfilename even with save points disabled. These
fixtures must neither load nor replace a developer's repository-local dump.rdb.
Persistence/restart tests instead supply and own their explicit --dir themselves.
"""
import subprocess
import tempfile


class IsolatedServer(subprocess.Popen):
    def __init__(self, args, **kwargs):
        self.data_dir = tempfile.TemporaryDirectory(prefix='peadb-test-server-')
        try:
            super().__init__([*args, '--dir', self.data_dir.name], **kwargs)
        except BaseException:
            self.data_dir.cleanup()
            raise

    def wait(self, *args, **kwargs):
        result = super().wait(*args, **kwargs)
        self.data_dir.cleanup()
        return result
