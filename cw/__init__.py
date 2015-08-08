from collections import namedtuple, OrderedDict
from cloud import serialization
import bluelet
import marshal
import random
import distutils.spawn
import cw.mp
import cw.slurm


PORT = 5494
# Some random bytes to separate messages.
SENTINEL = b'\x8d\xa9 \xee\x01\xe6B\xec\xaa\n\xe1A:\x15\x8d\x1b'


def is_slurm_available():
    return distutils.spawn.find_executable('salloc') != None


def start(nworkers, master=True, workers=True, master_options=[],
          worker_options=[], docker_image=None, docker_args=""):
    """
    Start up a cluster of workers, with autodetect: uses Slurm
    if available, otherwise defaults to launching all processes locally.
    
    - Only one cluster should be in operation at a given time 
      (only one 'cmaster').
    - If no docker_image is specified, docker will not be used.
    - Note that in the case of using local processes, master_options 
      and worker_options are ignored.
    - To explicitly start an mp or slurm cluster (no auto-detect)
      use the submodule versions: `cw.mp.start()` or `cw.slurm.start().
    """
    if is_slurm_available():
        cw.slurm.start(nworkers, master, workers, master_options,
                       worker_options, docker_image, docker_args)
    else:
        cw.mp.start(nworkers, master, workers)
        

def stop(master=True, workers=True):
    """Stop the running of a cluster (shut down cmaster and all cworkers).
       If slurm is available, this assumes slurm was used to start the
       cluster. To explicitly stop mp or slurm cluster, use the submodule
       versions: `cw.mp.stop()` or `cw.slurm.stop().
    """
    if is_slurm_available():
        cw.slurm.stop(master, workers)
    else:
        cw.mp.stop(master, workers)


def randid():
    return random.getrandbits(128)


def lru_cache(size=128):
    """Function decorator that memoizes results in-memory.
    """
    def decorator(func):
        cache = OrderedDict()

        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            if key in cache:
                # Hit.
                result = cache.pop(key)
                cache[key] = result
                return result
            else:
                # Miss.
                result = func(*args, **kwargs)
                cache[key] = result
                if len(cache) > size:
                    # Eviction.
                    del cache[cache.iterkeys().next()]
                return result

        return wrapper
    return decorator


# User data serialization.

def slow_ser(obj):
    """Serialize a complex object (like a closure)."""
    return serialization.serialize(obj, True)


def slow_deser(blob):
    """Deserialize a complex object."""
    return serialization.deserialize(blob)


@lru_cache()
def func_ser(obj):
    return slow_ser(obj)


@lru_cache()
def func_deser(blob):
    return slow_deser(blob)


# Messages.

TaskMessage = namedtuple(
    'TaskMessage',
    ['jobid', 'func_blob', 'args_blob', 'kwargs_blob', 'cwd', 'syspath']
)


ResultMessage = namedtuple(
    'ResultMessage',
    ['jobid', 'success', 'result_blob']
)


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
    if data is None or SENTINEL not in data:
        # `data` can be None because of a questionable decision in
        # bluelet to return None from a socket operation when it raises
        # an exception. I should fix this sometime.
        yield bluelet.end()  # Socket closed.
    data = data[:-len(SENTINEL)]
    obj = _msg_deser(data)
    yield bluelet.end(obj)
