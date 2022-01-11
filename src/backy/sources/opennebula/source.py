import json
import logging
import subprocess
import time

from ..ceph.source import CephRBD

SNAP_CREATE_SCRIPT = '/var/lib/one/remotes/tm/ceph/snap_create_live'
SNAP_DELETE_SCRIPT = '/var/lib/one/remotes/tm/ceph/snap_delete'
LOG = logging.getLogger(__name__)


def rbd_volume(vm_id, diskinfo):
    """Derive RBD image name from `onevm show` output.

    This function must be fed with a single record from the VM info's DISK
    array. Works only for OS or persistent VM disks. Volatile disks, even when
    living in the Ceph storage, don't have a SOURCE attribute.
    """
    if diskinfo['TYPE'] != 'RBD':
        raise RuntimeError(
            'Trying to derive rbd volume name from non-RBD', diskinfo)
    (pool, image) = diskinfo['SOURCE'].split('/', 2)
    if diskinfo['CLONE'] == 'YES':
        image = f'{image}-{vm_id}-{diskinfo["DISK_ID"]}'
    return (pool, image)


def find_disk(vm_id, disk_id, disks):
    for d in disks:
        if d['TYPE'] != 'RBD':
            continue
        if int(d['DISK_ID']) == disk_id:
            return (d['DATASTORE_ID'],) + rbd_volume(vm_id, d)


class OpenNebulaDisk(CephRBD):

    last_host = None

    def __init__(self, config):
        self.vm_id = config['vm_id']
        self.disk_id = config['disk_id']
        LOG.debug('Querying VM %d for disk %d', self.vm_id, self.disk_id)
        # Environment variables typically required: ONE_XMLRPC, ONE_AUTH
        vminfo = subprocess.run(
            ['onevm', 'show', '-j', str(self.vm_id)],
            capture_output=True, check=True)
        vm = json.loads(vminfo.stdout)['VM']
        self.vm_name = vm['NAME']
        try:
            self.last_host = vm['HISTORY_RECORDS']['HISTORY'][-1]['HOSTNAME']
        except IndexError:
            # VM has never been deployed -> skip
            pass
        try:
            (ds_id, pool, image) = find_disk(
                self.vm_id, self.disk_id, vm['TEMPLATE']['DISK'])
        except TypeError:
            raise RuntimeError('(%d) disk_id %d not found', self.vm_id,
                               self.disk_id)
        self.ds_id = ds_id
        super(OpenNebulaDisk, self).__init__({'pool': pool, 'image': image})

    @classmethod
    def config_from_cli(cls, spec):
        LOG.debug('OpenNebulaDisk.config_from_cli(%s)', spec)
        try:
            (vm_id, disk_id) = tuple(s.strip() for s in spec.split(',', 2))
        except ValueError:
            raise RuntimeError('OpenNebula source must be initialized with '
                               'VM_ID,DISK_ID')
        return {'vm_id': int(vm_id), 'disk_id': int(disk_id)}

    def ready(self):
        if not self.last_host:
            # VM has never been deployed
            return False
        return super(OpenNebulaDisk, self).ready()

    def create_snapshot(self, snapname):
        LOG.info('Creating snapshot %s for VM %d disk %d', snapname,
                 self.vm_id, self.disk_id)
        subprocess.run([
            'sudo', '-uoneadmin',
            SNAP_CREATE_SCRIPT, f'{self.last_host}:disk.{self.disk_id}',
            snapname, str(self.vm_id), str(self.ds_id)], check=True)

    def _delete_old_snapshots(self):
        # copied and adapted from CephRBD._delete_old_snapshots()
        if not self.always_full and self.revision.backup.history:
            keep_snapshot_revision = self.revision.backup.history[-1]
            keep_snapshot_revision = keep_snapshot_revision.uuid
        else:
            keep_snapshot_revision = None
        rbdimg = self._image_name
        for snapshot in self.rbd.snap_ls(self._image_name):
            snap = snapshot['name']
            if not snap.startswith('backy-'):
                # Do not touch non-backy snapshots
                continue
            uuid = snap.replace('backy-', '')
            if uuid != keep_snapshot_revision:
                time.sleep(3)  # avoid race condition while unmapping
                fullname = f'{rbdimg}@{snap}'
                LOG.info('Removing old snapshot %s', fullname)
                try:
                    self.rbd.snap_unprotect(fullname)
                    self.rbd.snap_rm(fullname)
                except Exception:
                    LOG.exception('Could not delete snapshot %s', fullname)
