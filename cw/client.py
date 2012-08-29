from __future__ import print_function
import cw
import bluelet
import threading
import concurrent.futures

class Client(object):
    def __init__(self, host='localhost', port=cw.PORT):
        self.host = host
        self.port = port

    def connection_ready(self):
        pass

    def handle_results(self, callback):
        self.conn = yield bluelet.connect(self.host, self.port)
        self.connection_ready()

        while True:
            result = yield cw._readmsg(self.conn)
            if result is None:
                print('server connection closed')
                return
            assert isinstance(result, cw.ResultMessage)

            callback(result.jobid, result.success, result.result)

    def send_job(self, jobid, func, *args, **kwargs):
        task = cw.TaskMessage(jobid, func, args, kwargs)
        yield cw._sendmsg(self.conn, task)

class ClientThread(threading.Thread, Client):
    def __init__(self, callback, host='localhost', port=cw.PORT):
        threading.Thread.__init__(self)
        Client.__init__(self, host, port)
        self.callback = callback
        self.daemon = True

        self.ready_condition = threading.Condition()
        self.ready = False

    def connection_ready(self):
        with self.ready_condition:
            self.ready = True
            self.ready_condition.notify_all()

    def run(self):
        # Receive on the socket in this thread.
        bluelet.run(self.handle_results(self.callback))

    def start_job(self, jobid, func, *args, **kwargs):
        # Synchronously send on the socket in the *calling* thread.
        with self.ready_condition:
            while not self.ready:
                self.ready_condition.wait()
        bluelet.run(self.send_job(jobid, func, *args, **kwargs))

class RemoteException(Exception):
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return '\n' + self.error.strip()

class ClusterExecutor(concurrent.futures.Executor):
    def __init__(self, host='localhost', port=cw.PORT):
        self.thread = ClientThread(self._completion, host, port)
        self.thread.start()

        self.futures = {}

        self.jobs_lock = threading.Lock()
        self.jobs_empty_cond = threading.Condition(self.jobs_lock)

    def _completion(self, jobid, success, result):
        with self.jobs_lock:
            future = self.futures.pop(jobid)
            if not self.futures:
                self.jobs_empty_cond.notify_all()

        if success:
            future.set_result(result)
        else:
            future.set_exception(RemoteException(result))

    def submit(self, func, *args, **kwargs):
        future = concurrent.futures.Future()

        jobid = cw.random_string()
        with self.jobs_lock:
            self.futures[jobid] = future
        self.thread.start_job(jobid, func, *args, **kwargs)

        return future

    def shutdown(self, wait=True):
        if wait:
            with self.jobs_lock:
                if self.futures:
                    self.jobs_empty_cond.wait()

        #FIXME stop thread

def test():
    def square(n):
        return n * n

    with ClusterExecutor() as executor:
        for res in executor.map(square, range(10)):
            print(res)

if __name__ == '__main__':
    test()
