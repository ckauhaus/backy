from backy.backup import Archive
from backy.schedule import Schedule
from prettytable import PrettyTable
import asyncio
import backy.utils
import datetime
import hashlib
import os
import pkg_resources
import random
import sys
import telnetlib3
import yaml

original_print = print


def print(str):
    original_print('{}: {}'.format(datetime.datetime.now().isoformat(), str))


class Task(object):
    """A single backup task with a specific set of tags to be executed
    at a specific time.
    """

    ideal_start = None
    tags = None

    def __init__(self, job):
        self.job = job
        self.tags = set()
        self.finished = asyncio.Event()

    @property
    def name(self):
        return self.job.name

    # Maybe this should be run in an executor instead?
    @asyncio.coroutine
    def backup(self, future):
        future.add_done_callback(lambda f: self.finished.set())

        print("{}: running backup {}, was due at {}".format(
            self.job.name, ', '.join(self.tags), self.ideal_start.isoformat()))

        # Update config
        # TODO: this isn't true async, but it works for now.
        backup_dir = os.path.join(
            self.job.daemon.config['global']['base-dir'],
            self.name)
        if not os.path.exists(backup_dir):
            # We do not want to create leading directories, only
            # the backup directory itself. If the base directory
            # does not exist then we likely don't have a correctly
            # configured environment.
            os.mkdir(backup_dir)
        with open(os.path.join(backup_dir, 'config'), 'w') as f:
            f.write(yaml.dump(self.job.source))

        # Run backup command
        cmd = "{} -b {} backup {}".format(
            self.job.daemon.backy_cmd,
            backup_dir,
            ','.join(self.tags))
        process = yield from asyncio.create_subprocess_shell(cmd)
        yield from process.communicate()

        # Expire backups
        # TODO: this isn't true async, but it works for now.
        self.job.archive.scan()
        self.job.schedule.expire(self.job.archive)

        print("{}: finished backup.".format(self.job.name))

        future.set_result(self)

    @asyncio.coroutine
    def wait_for_deadline(self):
        while self.ideal_start > backy.utils.now():
            remaining_time = backy.utils.now() - self.ideal_start
            yield from asyncio.sleep(remaining_time.total_seconds())

    @asyncio.coroutine
    def wait_for_finished(self):
        yield from self.finished.wait()


class TaskPool(object):
    """Continuously processes tasks by assigning workers from a limited pool
    ASAP.

    Chooses the next task based on its relative priority (it's ideal
    start date).

    Any task inserted into the pool will be worked on ASAP. Tasks are not
    checked whether their deadline is due.
    """

    def __init__(self, limit=2):
        self.tasks = asyncio.PriorityQueue()
        self.workers = asyncio.BoundedSemaphore(limit)

    @asyncio.coroutine
    def put(self, task):
        # Insert into a queue first, so that we can retrieve
        # them and order them at the appropriate time.
        yield from self.tasks.put((task.ideal_start, task))

    @asyncio.coroutine
    def get(self):
        priority, task = yield from self.tasks.get()
        return task

    @asyncio.coroutine
    def run(self):
        print("Starting to work on queue")
        while True:
            # Ok, lets get a worker slot and a task
            yield from self.workers.acquire()
            task = yield from self.get()
            task_future = asyncio.Future()
            # Now, lets select a job
            asyncio.async(task.backup(task_future))
            task_future.add_done_callback(self.finish_task)

    def finish_task(self, future):
        self.workers.release()
        task = future.result()
        print("{}: finished, releasing worker.".format(task.name))


class Job(object):

    name = None
    source = None
    schedule_name = None
    status = None

    _generator_handle = None

    def __init__(self, daemon, name):
        self.daemon = daemon
        self.name = name
        self.handle = None

    def configure(self, config):
        self.source = config['source']
        self.schedule_name = config['schedule']
        self.path = self.daemon.base_dir + '/' + self.name
        self.archive = Archive(self.path)
        self.daemon.loop.call_soon(self.start)

    @property
    def spread(self):
        seed = int(hashlib.md5(self.name.encode('utf-8')).hexdigest(), 16)
        limit = max(x['interval'] for x in self.schedule.schedule.values())
        limit = limit.total_seconds()
        generator = random.Random()
        generator.seed(seed)
        return generator.randint(0, limit)

    @property
    def sla(self):
        """Is the SLA currently held?

        The SLA being held is only reflecting the current status.

        It does not help to reflect on past situations that have failed as
        those are not indicators whether and admin needs to do something
        right now.
        """
        max_age = min(x['interval'] for x in self.schedule.schedule.values())
        self.archive.scan()
        if not self.archive.history:
            return True
        newest = self.archive.history[-1]
        age = backy.utils.now() - newest.timestamp
        if age > max_age * 1.5:
            return False
        return True

    @property
    def schedule(self):
        return self.daemon.schedules[self.schedule_name]

    def update_status(self, status):
        self.status = status
        print('{}: {}'.format(self.name, self.status))

    @asyncio.coroutine
    def generate_tasks(self):
        """Generate backup tasks for this job.
         tasks based on the ideal next time in the future
        and previous tasks to ensure we catch up quickly if the next
        job in the future is too far away.

        This may repetetively submit a task until its time has come and then
        generate other tasks after the ideal next time has switched over.

        It doesn't care whether the tasks have been successfully worked on or
        not. The task pool needs to deal with that.
        """
        print("{}: started task generator loop".format(self.name))
        while True:
            next_time, next_tags = self.schedule.next(
                backy.utils.now(), self.spread, self.archive)

            self.task = task = Task(self)
            self.task.ideal_start = next_time
            self.task.tags.update(next_tags)

            self.update_status("waiting")
            yield from task.wait_for_deadline()
            self.update_status("submitting to worker queue")
            yield from self.daemon.taskpool.put(task)
            # XXX This is a lie
            self.update_status("running")
            yield from task.wait_for_finished()
            self.update_status("finished")

    def start(self):
        self.stop()
        self._generator_handle = asyncio.async(self.generate_tasks())

    def stop(self):
        if self._generator_handle:
            self._generator_handle.cancel()
            self._generator_handle = None


class BackyDaemon(object):

    worker_limit = 1
    base_dir = '/srv/backy'

    def __init__(self, loop, config_file):
        self.backy_cmd = os.path.join(
            os.getcwd(), os.path.dirname(sys.argv[0]), 'backy')
        self.loop = loop
        self.config_file = config_file
        self.config = None
        self.schedules = {}
        self.jobs = {}

    def configure(self):
        # XXX Signal handling to support reload
        if not os.path.exists(self.config_file):
            print('Could not load configuration. '
                  '`{}` does not exist.'.format(self.config_file))
            raise SystemExit(1)
        with open(self.config_file, 'r', encoding='utf-8') as f:
            config = yaml.load(f)
        if config != self.config:
            print("Config changed - reloading.")
            self.config = config
            self.configure_global()
            self.configure_schedules()
            self.configure_jobs()

    def configure_global(self):
        c = self.config.get('global')
        if c:
            self.worker_limit = c.get('worker-limit', self.worker_limit)
        # This isn't reload-safe. The semaphore is tricky to update
        self.taskpool = TaskPool(self.worker_limit)
        print("New worker limit: {}".format(self.worker_limit))
        self.base_dir = c.get('base-dir', self.base_dir)
        print("New backup location: {}".format(self.base_dir))

    def configure_schedules(self):
        # TODO: Ensure that we do not accidentally remove schedules that
        # are still in use? Or simply warn in this case?
        # Use monitoring for that?
        new = {}
        for name, config in self.config['schedules'].items():
            if name in self.schedules:
                new[name] = self.schedules[name]
            else:
                new[name] = Schedule()
            new[name].configure(config)
        self.schedules = new
        print("Available schedules: {}".format(', '.join(self.schedules)))

    def configure_jobs(self):
        new = {}
        for name, config in self.config['jobs'].items():
            if name in self.jobs:
                job = self.jobs[name]
            else:
                job = Job(self, name)
            new[name] = job
            new[name].configure(config)
        # Stop old jobs.
        for job in self.jobs.values():
            if job not in new:
                job.stop()
        # (Re-)start existing and new jobs.
        for job in new.values():
            job.start()
        self.jobs = new
        print("Configured jobs: {}".format(len(self.jobs)))

    def start(self):
        self.configure()
        asyncio.async(self.taskpool.run())


class SchedulerShell(telnetlib3.Telsh):

    shell_name = 'backy'
    shell_ver = pkg_resources.require("backy")[0].version

    def cmdset_jobs(self):
        t = PrettyTable(["Job",
                         "SLA",
                         "Status",
                         "Last Backup",
                         "Last Tags",
                         "Last Duration",
                         "Next Backup",
                         "Next Tags"])

        for job in daemon.jobs.values():
            job.archive.scan()
            if job.archive.history:
                last = job.archive.history[-1]
            else:
                last = None
            t.add_row([job.name,
                       'OK' if job.sla else 'TOO OLD',
                       job.status,
                       (backy.utils.format_timestamp(last.timestamp)
                        if last else '-'),
                       (', '.join(job.schedule.sorted_tags(last.tags))
                        if last else '-'),
                       (datetime.timedelta(seconds=last.stats['duration'])
                        if last else '-'),
                       backy.utils.format_timestamp(job.task.ideal_start),
                       ', '.join(job.schedule.sorted_tags(job.task.tags))])

        t.sortby = "Job"
        t.align = 'l'

        self.stream.write(t.get_string().replace('\n', '\r\n'))

    def cmdset_status(self):
        t = PrettyTable(["Property", "Status"])
        t.add_row(["Idle Workers", daemon.taskpool.workers._value + 1])
        t.sortby = "Property"
        self.stream.write(t.get_string().replace('\n', '\r\n'))


daemon = None


def main(config_file):
    global daemon

    loop = asyncio.get_event_loop()
    daemon = BackyDaemon(loop, config_file)

    daemon.start()

    func = loop.create_server(
        lambda: telnetlib3.TelnetServer(shell=SchedulerShell),
        '127.0.0.1', 6023)
    loop.run_until_complete(func)

    # Blocking call interrupted by loop.stop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
