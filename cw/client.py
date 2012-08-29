from __future__ import print_function
import cw
import bluelet

class Client(object):
    def __init__(self, host='localhost', port=cw.PORT):
        self.host = host
        self.port = port
        self.pending = {}  # {jobid: suspended coro}

    def communicate(self):
        self.conn = yield bluelet.connect(self.host, self.port)
        while True:
            result = yield cw._readmsg(self.conn)
            if result is None:
                print('server connection closed')
                return
            assert isinstance(result, cw.ResultMessage)

            # NOW REAWAKEN result.jobid
            # AT THIS POINT I NEED WAIT/NOTIFY

    def call(self, func, *args, **kwargs):
        jobid = cw.random_string()
        task = cw.TaskMessage(jobid, func, args, kwargs)
        yield cw._sendmsg(self.conn, task)

        # NOW WAIT...?

def bluelet_test():
    client = Client()
    yield bluelet.spawn(client.communicate())

    def square(n):
        return n ** n

    def print_square(n):
        res = yield bluelet.call(client.call(square, n))
        print(n, 'squared is', res)

    for i in range(10):
        yield bluelet.spawn(print_square(i))

if __name__ == '__main__':
    bluelet.run(bluelet_test())
