from backy.utils import SafeWritableFile
from subprocess import check_output
import glob
import json
import logging
import os
import sys
import time
import uuid

logger = logging.getLogger(__name__)
cmd = check_output


if sys.platform == 'linux2':
    def cp_reflink(source, target):
        cmd('cp --reflink=always {} {}'.
            format(source, target), shell=True)
else:
    def cp_reflink(source, target):
        logger.warn('Performing non-COW copy: {} -> {}'.format(source, target))
        cmd('cp {} {}'. format(source, target), shell=True)


class Revision(object):

    uuid = None
    timestamp = None
    parent = None
    stats = None

    def __init__(self, uuid, backup):
        self.uuid = uuid
        self.backup = backup
        self.stats = {}

    @classmethod
    def create(cls, backup):
        r = Revision(str(uuid.uuid4()), backup)
        r.timestamp = time.time()
        return r

    @classmethod
    def load(cls, file, backup):
        metadata = json.load(open(file, 'r', encoding='utf-8'))
        r = Revision(metadata['uuid'], backup)
        r.timestamp = metadata['timestamp']
        r.parent = metadata['parent']
        r.stats = metadata.get('stats', {})
        return r

    @property
    def filename(self):
        return '{}/{}'.format(self.backup.path, self.uuid)

    @property
    def info_filename(self):
        return self.filename + '.rev'

    def materialize(self):
        self.write_info()
        if not self.parent:
            open(self.filename, 'wb').close()
            return

        parent = self.backup.find_revision(self.parent)
        cp_reflink(parent.filename, self.filename)

    def write_info(self):
        metadata = {
            'uuid': self.uuid,
            'timestamp': self.timestamp,
            'parent': self.parent,
            'stats': self.stats}
        with SafeWritableFile(self.info_filename) as f:
            d = json.dumps(metadata)
            f.write(d.encode('utf-8'))

    def set_link(self, name):
        path = self.backup.path
        if os.path.exists(path+'/'+name):
            os.unlink(path+'/'+name)
        os.symlink(os.path.relpath(self.filename, path), path+'/'+name)

        name = name + '.rev'
        if os.path.exists(path+'/'+name):
            os.unlink(path+'/'+name)
        os.symlink(os.path.relpath(self.info_filename, path), path+'/'+name)

    def remove(self):
        for file in glob.glob(self.filename+'*'):
            os.unlink(file)
