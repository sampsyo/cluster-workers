from __future__ import print_function
import cw
import cw.slurm
import bluelet
import traceback
import sys
import os
from contextlib import contextmanager


def format_remote_exc():
    typ, value, tb = sys.exc_info()
    tb = tb.tb_next  # Remove root call to worker().
    return ''.join(traceback.format_exception(typ, value, tb))


@contextmanager
def chdir(d):
    """Enter a directory for the duration of the context manager.
    """
    olddir = os.getcwd()
    os.chdir(d)
    yield
    os.chdir(olddir)


@contextmanager
def extend_path(dirs):
    """Extend sys.path with some new directories (at the front of the
    list) for the duration of the context manager.
    """
    old_syspath = list(sys.path)
    for entry in dirs:
        if entry not in sys.path:
            sys.path.insert(0, entry)
    yield
    sys.path = old_syspath


def amend_path():
    # This ridiculous bit of hackery ensures that the modules under the
    # cw package can themselves be used by jobs. In the case that the
    # workers are run from the directory containing cw, cw.__path__ will
    # be ['cw']. Then, after cwd'ing to run a job, it will no longer be
    # possible to find that path. Absolute-ifying the package path makes
    # it relocatable.
    cw.__path__ = map(os.path.abspath, cw.__path__)


class Worker(object):
    def __init__(self, host='localhost', port=cw.PORT):
        self.host = host
        self.port = port

    def communicate(self):
        conn = yield bluelet.connect(self.host, self.port)

        yield cw._sendmsg(conn, cw.WorkerRegisterMessage())

        connected = True
        try:
            while True:
                # Get a task from the master.
                msg = yield cw._readmsg(conn)
                if msg is None:
                    print('connection to master closed')
                    connected = False
                    break
                assert isinstance(msg, cw.TaskMessage)

                try:
                    with chdir(msg.cwd):
                        with extend_path(msg.syspath):
                            func = cw.func_deser(msg.func_blob)
                            args = cw.slow_deser(msg.args_blob)
                            kwargs = cw.slow_deser(msg.kwargs_blob)
                            res = func(*args, **kwargs)
                except:
                    res = format_remote_exc()
                    response = cw.ResultMessage(msg.jobid, False,
                                                cw.slow_ser(res))
                else:
                    response = cw.ResultMessage(msg.jobid, True,
                                                cw.slow_ser(res))
                yield cw._sendmsg(conn, response)

        finally:
            if connected:
                yield cw._sendmsg(conn, cw.WorkerDepartMessage())

    def run(self):
        amend_path()
        try:
            bluelet.run(self.communicate())
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    args = sys.argv[1:]

    if args and args[0] == '--slurm':
        host = cw.slurm.master_host()
    elif args:
        host = args[0]
    else:
        host = 'localhost'

    Worker(host).run()
