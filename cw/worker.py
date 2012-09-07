from __future__ import print_function
import cw
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
    olddir = os.getcwd()
    os.chdir(d)
    yield
    os.chdir(olddir)

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
                    func = cw.func_deser(msg.func_blob)
                    args = cw.slow_deser(msg.args_blob)
                    kwargs = cw.slow_deser(msg.kwargs_blob)
                    with chdir(msg.cwd):
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
        try:
            bluelet.run(self.communicate())
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    args = sys.argv[1:]

    if args and args[0] == '--slurm':
        host = cw.slurm_master_host()
    elif args:
        host = args[0]
    else:
        host = 'localhost'

    Worker(host).run()
