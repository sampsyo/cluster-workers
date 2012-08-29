from collections import namedtuple
from cloud import serialization
import bluelet
import string
import random

PORT = 5494
# Some random bytes to separate messages.
SENTINEL = b'\x8d\xa9 \xee\x01\xe6B\xec\xaa\n\xe1A:\x15\x8d\x1b'

TaskMessage = namedtuple('TaskMessage', ['jobid', 'func', 'args', 'kwargs'])
ResultMessage = namedtuple('ResultMessage', ['jobid', 'result'])
class WorkerRegisterMessage(object):
    pass
class WorkerDepartMessage(object):
    pass

def _sendmsg(conn, obj):
    yield conn.sendall(serialization.serialize(obj, True) + SENTINEL)

def _readmsg(conn):
    data = yield conn.readline(SENTINEL)
    if SENTINEL not in data:
        yield bluelet.end()  # Socket closed.
    data = data[:-len(SENTINEL)]
    obj = serialization.deserialize(data)
    yield bluelet.end(obj)

def random_string(length=32, chars=(string.ascii_letters + string.digits)):
    return ''.join(random.choice(chars) for i in range(length))
