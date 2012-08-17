import cw
import bluelet

class Client(object):
    def __init__(self, host='localhost', port=cw.PORT):
        self.host = host
        self.port = port

    def communicate(self):
        conn = yield bluelet.connect(self.host, self.port)

        def hello():
            return 'SUP DOG'
        task = cw.TaskMessage('foobar', hello, [], {})
        yield cw._sendmsg(conn, task)
        result = yield cw._readmsg(conn)
        if result is None:
            print('server connection closed')
            return
        assert isinstance(result, cw.ResultMessage)
        print(result.result)

    def run(self):
        try:
            bluelet.run(self.communicate())
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    Client().run()
