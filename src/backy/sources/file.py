from backy.utils import safe_copy, compare_files


class File(object):

    def __init__(self, config):
        self.filename = config['filename']

    @staticmethod
    def config_from_cli(spec):
        return dict(filename=spec)

    def __call__(self, revision):
        self.revision = revision
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        pass

    def backup(self):
        t = open(self.revision.filename, 'wb')
        s = open(self.filename, 'rb')

        with s as source, t as target:
            bytes = safe_copy(source, target)
        self.revision.stats['bytes_written'] = bytes

    def verify(self):
        s = open(self.filename, 'rb')
        t = open(self.revision.filename, 'rb')

        with s as source, t as target:
            return compare_files(source, target)
