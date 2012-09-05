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

# Represents a delayed function call with all its components. This is
# separate from a TaskMessage so that it does not need to be
# unpacked/repacked on the master -- only the metadata needs that.
Call = namedtuple('Call', ['func', 'args', 'kwargs', 'cwd'])

def call_ser(call):
    """Serialize a Call to an opaque binary blob."""
    return serialization.serialize(call, True)

def call_deser(blob):
    """Deserialize a Call from a blob."""
    return serialization.deserialize(blob)


# Messages.

TaskMessage = namedtuple('TaskMessage', ['jobid', 'call_blob'])

ResultMessage = namedtuple('ResultMessage', ['jobid', 'success', 'result'])

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
        ['squeue', '-j', str(jobid), '-o', '%i %j %N', '-h']
    )
    for line in joblist.strip().split('\n'):
        jobid, name, nodelist = line.split(' ', 2)
        return int(jobid), name, nodelist

def slurm_master_host():
    for jobid, name, nodelist in slurm_jobinfo():
        if name == JOB_MASTER:
            assert '[' not in nodelist
            return nodelist
    assert False, 'no master job found'
