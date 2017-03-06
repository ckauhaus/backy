from backy.scheduler import Task
from unittest import mock
import backy.utils
import datetime
import pytest


def test_task():
    job = mock.Mock()
    job.name = 'test01'

    t = Task(job)

    assert t.ideal_start is None
    assert t.tags == set()
    assert t.name == 'test01'


@pytest.mark.asyncio
def test_wait_for_deadline_no_deadline_fails():
    job = mock.Mock()
    t = Task(job)
    # Not having a a deadline set causes this to fail.
    with pytest.raises(TypeError):
        yield from t.wait_for_deadline()


@pytest.mark.asyncio
def test_wait_for_deadline():
    job = mock.Mock()
    t = Task(job)
    # Not having a a deadline set causes this to fail.
    now = backy.utils.now()
    t.ideal_start = now + datetime.timedelta(seconds=0.1)
    yield from t.wait_for_deadline()
    assert backy.utils.now() - now >= datetime.timedelta(seconds=0.1)
