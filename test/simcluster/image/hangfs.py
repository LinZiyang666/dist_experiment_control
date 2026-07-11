#!/usr/bin/env python3
# hangfs.py — a minimal passthrough FUSE (fstype `fuse.hangfs`) used by drill 62 to reproduce a
# HUNG network-style mount on the real stack. It answers statfs/getattr/read NORMALLY while its daemon
# runs, so the agent's boot-time mount scan classifies /mnt/hung as a hangable mount (spawnsafe
# classifyFstype: prefix "fuse." → kindRemoteProbe, internal/spawnsafe/spawnsafe.go). SIGSTOP'ing the
# daemon then blocks EVERY kernel request incl. statfs — the wedge the agent's bounded probe must catch.
#
# It is a REAPABLE (S/T-state, kill-9-able) APPROXIMATION of a wedged mount, NOT true uninterruptible-D
# (that needs kernel nfsd + a hard mount, a shared-host hazard). Drill 62 measures this distinction with
# a /proc/<pid>/stat + bounded kill-9 discriminator and labels the arms accordingly (true-D = NOT-COVERED).
#
# Usage: hangfs.py <mountpoint>   (foreground; SIGSTOP the process to wedge it, SIGCONT to heal)
import sys
import stat
import time
from fusepy import FUSE, Operations


class HangFS(Operations):
    def getattr(self, path, fh=None):
        now = time.time()
        if path == "/":
            return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2,
                        st_ctime=now, st_mtime=now, st_atime=now)
        # every non-root path is a 0-byte, executable regular file (so an absolute argv[0] like
        # /mnt/hung/probe resolves during a healthy window and blocks once the daemon is stopped).
        return dict(st_mode=(stat.S_IFREG | 0o755), st_nlink=1, st_size=0,
                    st_ctime=now, st_mtime=now, st_atime=now)

    def readdir(self, path, fh):
        return [".", "..", "probe"]

    def statfs(self, path):
        return dict(f_bsize=4096, f_frsize=4096, f_blocks=1024, f_bfree=1024, f_bavail=1024,
                    f_files=16, f_ffree=16, f_favail=16, f_namemax=255)

    def open(self, path, flags):
        return 0

    def read(self, path, size, offset, fh):
        return b""

    def access(self, path, mode):
        return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.stderr.write("usage: hangfs.py <mountpoint>\n")
        sys.exit(2)
    FUSE(HangFS(), sys.argv[1], foreground=True, nothreads=True,
         allow_other=True, ro=True, subtype="hangfs")
