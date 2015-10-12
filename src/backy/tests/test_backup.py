from backy.backup import Backup
from backy.revision import Revision
from backy.sources.file import File
import backy.utils
import yaml
import os.path
import pytest
import shutil


fixtures = os.path.dirname(__file__) + '/samples'


@pytest.fixture
def simple_file_config(tmpdir, monkeypatch):
    shutil.copy(fixtures + '/simple_file/config', str(tmpdir))
    monkeypatch.chdir(tmpdir)
    b = Backup(str(tmpdir))
    b.configure()
    return b


def test_config(simple_file_config, tmpdir):
    backup = simple_file_config

    assert backup.path == str(tmpdir)
    assert isinstance(backup.source, File)
    assert backup.source.filename == "input-file"


def test_init_ceph(tmpdir):
    backup = Backup(str(tmpdir))
    backup.init('ceph-rbd', 'test/test04')
    with open(str(tmpdir / 'config')) as f:
        config = yaml.safe_load(f)
    assert config == {
        "type": "ceph-rbd",
        "image": "test04",
        "pool": "test"}


def test_init_file(tmpdir):
    backup = Backup(str(tmpdir))
    backup.init('file', '/dev/foo')

    with open(str(tmpdir / 'config')) as f:
        config = yaml.load(f)
    assert config == {
        "type": "file",
        "filename": "/dev/foo"}


def test_init_creates_directory(tmpdir):
    target = str(tmpdir / 'asdf')
    assert not os.path.exists(target)
    backup = Backup(target)
    backup.init('file', '/dev/foo')
    assert os.path.isdir(target)


def test_init_refused_with_existing_config(tmpdir):
    existing_config = b"""\
{"source-type": "file", "source": {"filename": "asdf"}, "marker": 1}\
"""
    with open(str(tmpdir / 'config'), 'wb') as f:
        f.write(existing_config)
    backup = Backup(str(tmpdir))
    with pytest.raises(RuntimeError):
        backup.init('file', '/dev/foo')
    with open(str(tmpdir / 'config'), 'rb') as f:
        assert f.read() == existing_config


def test_find(simple_file_config, tmpdir):
    backup = simple_file_config
    rev = Revision('123-456', backup.archive)
    rev.timestamp = backy.utils.now()
    rev.materialize()
    assert str(tmpdir / '123-456') == backup.find(0)


def test_find_should_raise_if_not_found(simple_file_config):
    backup = simple_file_config
    rev = Revision('123-456', backup.archive)
    rev.timestamp = backy.utils.now()
    rev.materialize()
    with pytest.raises(RuntimeError):
        backup.find('no such revision')


def test_restore_target(simple_file_config, forget_about_btrfs):
    backup = simple_file_config
    source = 'input-file'
    target = 'restore.img'
    with open(source, 'wb') as f:
        f.write(b'volume contents\n')
    backup.backup('daily')
    backup.restore(0, target)
    with open(source, 'rb') as s, open(target, 'rb') as t:
        assert s.read() == t.read()


def test_restore_stdout(simple_file_config, forget_about_btrfs, capfd):
    backup = simple_file_config
    source = 'input-file'
    with open(source, 'wb') as f:
        f.write(b'volume contents\n')
    backup.backup('daily')
    backup.restore(0, '-')
    assert not os.path.exists('-')
    out, err = capfd.readouterr()
    assert 'volume contents\n' == out
    assert '' == err
