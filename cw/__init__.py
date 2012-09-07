from collections import namedtuple
from cloud import serialization
import bluelet
import marshal
import random
import subprocess

PORT = 5494
# Some random bytes to separate messages.
SENTINEL = b'\x8d\xa9 \xee\x01\xe6B\xec\xaa\n\xe1A:\x15\x8d\x1b'
JOB_MASTER ='cmaster'
JOB_WORKERS = 'cworkers'

def randid():
    return random.getrandbits(128)


# Function serialization.

def slow_ser(obj):
    """Serialize a complex object (like a closure)."""
    return serialization.serialize(obj, True)

def slow_deser(blob):
    """Deserialize a complex object."""
    return serialization.deserialize(blob)


# Messages.

TaskMessage = namedtuple('TaskMessage',
    ['jobid', 'func_blob', 'args_blob', 'kwargs_blob', 'cwd'])

ResultMessage = namedtuple('ResultMessage',
    ['jobid', 'success', 'result_blob'])

class WorkerRegisterMessage(object):
    pass

class WorkerDepartMessage(object):
    pass


# Fast serialization for messages & coroutines for sending/receiving.

def _msg_ser(msg):
    typename = type(msg).__name__
    if type(msg) in (WorkerRegisterMessage, WorkerDepartMessage):
        return typename
    elif isinstance(msg, (TaskMessage, ResultMessage)):
        return marshal.dumps((typename, tuple(msg)))
    else:
        assert False

def _msg_deser(text):
    if text in (WorkerRegisterMessage.__name__, WorkerDepartMessage.__name__):
        typ = globals()[text]
        return typ()
    else:
        typename, vals = marshal.loads(text)
        typ = globals()[typename]
        return typ(*vals)

def _sendmsg(conn, obj):
    yield conn.sendall(_msg_ser(obj) + SENTINEL)

def _readmsg(conn):
    data = yield conn.readline(SENTINEL)
    if SENTINEL not in data:
        yield bluelet.end()  # Socket closed.
    data = data[:-len(SENTINEL)]
    obj = _msg_deser(data)
    yield bluelet.end(obj)


# Slurm utilities.

def slurm_jobinfo():
    """Uses "squeue" to generate a list of job information tuples. The
    tuples are of the form (jobid, jobname, nodelist).
    """
    joblist = subprocess.check_output(
        ['squeue', '-o', '%i %j %N', '-h']
    ).strip()
    if not joblist:
        return
    for line in joblist.split('\n'):
        jobid, name, nodelist = line.split(' ', 2)
        yield int(jobid), name, nodelist

def slurm_master_host():
    for jobid, name, nodelist in slurm_jobinfo():
        if name == JOB_MASTER:
            assert '[' not in nodelist
            return nodelist
    assert False, 'no master job found'
