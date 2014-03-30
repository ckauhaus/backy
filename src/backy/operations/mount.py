from collections import defaultdict
import errno
from time import time
import backy.backup
import fuse
import logging
import os
import sys


fuse.fuse_python_api = (0, 2)


if not hasattr(__builtins__, 'bytes'):
    bytes = str


class RevisionFile(object):

    def __init__(self, path, flags, *mode):
        pass

    def read(self, length, offset):
        return ''

    def write(self, buf, offset):
        raise OSError('Not writable.')


class BackyFS(fuse.Fuse):

    file_class = RevisionFile

    def __init__(self, backupfile):
        fuse.Fuse.__init__(self)
        self.backup = backy.backup.Backup(backupfile)

    def getattr(self, path):
        """
        - st_mode (protection bits)
        - st_ino (inode number)
        - st_dev (device)
        - st_nlink (number of hard links)
        - st_uid (user ID of owner)
        - st_gid (group ID of owner)
        - st_size (size of file, in bytes)
        - st_atime (time of most recent access)
        - st_mtime (time of most recent content modification)
        - st_ctime (platform dependent; time of most recent metadata change on Unix,
                    or the time of creation on Windows).
        """
        print "*** getattr", path
        stat = fuse.Stat()
        stat.st_nlink = 1
        from stat import *
        if path == '/':
            stat.st_mode = S_IFDIR | 0555
        else:
            revision = path[1:]
            if not revision in self.backup.revisions:
                return -errno.ENOENT
            stat.st_mode = S_IFREG | 0444
            revision = self.backup.revisions[revision]
            stat.st_size = revision.size
            stat.st_mtime = revision.timestamp
        return stat

    def readdir(self, path, offset):
        print "*** readdir", path, offset
        assert path == '/'
        for revision in self.backup.revisions:
            yield fuse.Direntry(revision)

    def lock(self, path):
        print "*** lock"
        return -errno.EINVAL

    def statfs(self):
        print "*** statfs"
        s = fuse.StatVfs()
        s.f_bsize = 4*1024
        s.f_frsize  = s.f_bsize
        s.f_blocks  = 0 # XXX
        s.f_bfree   = 0
        s.f_bavail  = 0
        s.f_files   = len(self.backup.revisions)
        s.f_ffree   = 0
        s.f_favail  = 0
        s.f_flag    = 0
        s.f_namemax = 255
        return s


def mount(backupfile, mountpoint):
    logging.getLogger().setLevel(logging.DEBUG)
    fs = BackyFS(backupfile)
    # XXX meh.
    sys.argv = ['foo', '-d', mountpoint]
    fs.parse(errex=1)
    fs.main()
