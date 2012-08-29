import cw
import bluelet
import traceback
import sys

def format_remote_exc():
    typ, value, tb = sys.exc_info()
    tb = tb.tb_next  # Remove root call to worker().
    return ''.join(traceback.format_exception(typ, value, tb))

class Worker(object):
    def __init__(self, host='localhost', port=cw.PORT):
        self.host = host
        self.port = port

    def communicate(self):
        conn = yield bluelet.connect(self.host, self.port)

        yield cw._sendmsg(conn, cw.WorkerRegisterMessage())

        try:
            while True:
                # Get a task from the master.
                msg = yield cw._readmsg(conn)
                assert isinstance(msg, cw.TaskMessage)

                print('got a task')
                try:
                    res = msg.func(*msg.args, **msg.kwargs)
                except:
                    res = format_remote_exc()
                    response = cw.ResultMessage(msg.jobid, False, res)
                else:
                    response = cw.ResultMessage(msg.jobid, True, res)
                yield cw._sendmsg(conn, response)
                print('sent response')

        finally:
            yield cw._sendmsg(conn, cw.WorkerDepartMessage())

    def run(self):
        try:
            bluelet.run(self.communicate())
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    Worker().run()
