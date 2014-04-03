from backy.revision import Revision
from backy.source import Source
from backy.utils import SafeWritableFile
from glob import glob
import backy
import backy.fusefs
import datetime
import json
import os
import os.path
import sys


def format_timestamp(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


class Backup(object):

    CHUNKSIZE = 4 * 1024**2

    def __init__(self, path):
        # The path identifies the newest backup. Additional files
        # will be named with suffixes.
        self.path = os.path.realpath(path)
        self._configure()

    def _configure(self):
        if not os.path.exists(self.path + '/config'):
            return
        config = json.load(open(self.path + '/config', 'rb'))
        self.CHUNKSIZE = config['chunksize']
        source = config['source']
        source = os.path.join(self.path, source)
        self.source = Source(os.path.realpath(source), self)

    def _scan_revisions(self):
        self.revisions = {}
        # Load all revision infos
        for file in glob(self.path + '/*.rev'):
            r = Revision.load(file, self)
            self.revisions[r.uuid] = r

        self.revision_history = self.revisions.values()
        self.revision_history.sort(key=lambda r: r.timestamp)

    # Internal API

    def find_revision(self, spec):
        self._scan_revisions()
        if spec == 'last':
            return self.revision_history[-1].uuid

        try:
            spec = int(spec)
            return self.revision_history[-spec-1]
        except ValueError:
            return self.revisions[spec]

        # "goto fail" - should never come here.
        raise KeyError("Could not find revision %r" % spec)

    def find_revisions(self, spec):
        self._scan_revisions()
        if spec == 'all':
            result = self.revisions.values()
        else:
            result = [self.find_revision(spec)]
        return result

    # Command API

    def init(self, source):
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        if os.path.exists(self.path + '/config'):
            raise RuntimeError('Refusing initialize with existing config.')
        with SafeWritableFile(self.path+'/config') as f:
            json.dump({'chunksize': self.CHUNKSIZE, 'source': source},
                      f)
        self._configure()

    def status(self):
        self._scan_revisions()
        total_blocks = 0

        print "== Revisions"
        for r in self.revision_history:
            print "{}\t{}\t{}".format(
                format_timestamp(r.timestamp),
                len(r.blocksums),
                r.uuid)
            total_blocks += len(r.blocksums)

        print
        print "== Summary"
        print "{} revisions with {} blocks (~{} blocks/revision)".format(
            len(self.revisions),
            total_blocks,
            0 if not self.revisions else total_blocks/len(self.revisions))

    def backup(self):
        self._scan_revisions()

        if self.revision_history:
            previous = self.revision_history[-1]
            r = previous.migrate_to_delta()
        else:
            r = Revision.create('full', self)

        self.source.open()
        try:
            r.start(self.source.size)
            for index, chunk in self.source.iterchunks():
                r.store(index, chunk)
            r.stop()
        except:  # Intentional bare except to support reliable cleanup
            self.source.close()

        if os.path.exists(self.path+'/last'):
            os.unlink(self.path+'/last')
        os.symlink(os.path.relpath(r.filename, self.path),
                   self.path+'/last')

        if os.path.exists(self.path+'/last.rev'):
            os.unlink(self.path+'/last.rev')
        os.symlink(os.path.relpath(r.info_filename, self.path),
                   self.path+'/last.rev')

    def maintenance(self, keep):
        self._scan_revisions()
        for r in self.revision_history[:-keep]:
            print "Removing revision {}".format(r.uuid)
            r.remove()

    def restore(self, target, revision):
        self._scan_revisions()
        revision = self.find_revision(revision)
        # XXX safety belt
        print "Restoring revision {}".format(revision.uuid)
        revision.restore(target)

    def scrub(self, revision, markbad=False):
        self._scan_revisions()
        for r in self.find_revisions(revision):
            r.scrub(markbad)

    def mount(self, mountpoint):
        self._scan_revisions()
        fs = backy.fuse.BackyFS(self)
        # XXX meh.
        sys.argv = ['foo', '-d', mountpoint]
        fs.parse(errex=1)
        fs.main()
