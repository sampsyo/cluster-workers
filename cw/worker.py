import cw
import bluelet

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
                # TODO exception handling
                res = msg.func(*msg.args, **msg.kwargs)
                response = cw.ResultMessage(msg.jobid, res)
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
