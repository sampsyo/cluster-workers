from __future__ import print_function
import cw
import bluelet
import threading
import concurrent.futures
import os
import sys


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

            callback(result.jobid, result.success,
                     cw.slow_deser(result.result_blob))

    def send_job(self, jobid, func, *args, **kwargs):
        task = cw.TaskMessage(
            jobid,
            cw.func_ser(func), cw.slow_ser(args), cw.slow_ser(kwargs),
            os.getcwd(),
            sys.path,
        )
        yield cw._sendmsg(self.conn, task)


class BaseClientThread(threading.Thread, Client):
    def __init__(self, callback, host='localhost', port=cw.PORT):
        threading.Thread.__init__(self)
        Client.__init__(self, host, port)
        self.callback = callback
        self.daemon = True

        self.ready_condition = threading.Condition()
        self.ready = False

        self.shutdown = False
        self.shutdown_lock = threading.Lock()

    def connection_ready(self):
        with self.ready_condition:
            self.ready = True
            self.ready_condition.notify_all()

    def main_coro(self):
        handler = self.handle_results(self.callback)
        yield bluelet.spawn(handler)

        # Poll for thread shutdown.
        while True:
            yield bluelet.sleep(1)
            with self.shutdown_lock:
                if self.shutdown:
                    break

        # Halt the handler thread.
        yield bluelet.kill(handler)

    def stop(self):
        with self.shutdown_lock:
            self.shutdown = True

    def run(self):
        # Receive on the socket in this thread.
        bluelet.run(self.main_coro())

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


class ClientThread(BaseClientThread):
    """A slightly nicer ClientThread that generates job IDs for you and
    raises exceptions when things go wrong on the remote side.
    """
    def __init__(self, callback, host='localhost', port=cw.PORT):
        super(ClientThread, self).__init__(self._completion, host, port)
        self.app_callback = callback

        self.active_jobs = 0
        self.remote_exception = None
        self.jobs_cond = threading.Condition()

    def submit(self, jobid, func, *args, **kwargs):
        with self.jobs_cond:
            self.active_jobs += 1
        self.start_job(jobid, func, *args, **kwargs)

    def _completion(self, jobid, success, result):
        with self.jobs_cond:
            if success:
                self.app_callback(jobid, result)
                self.active_jobs -= 1
            else:
                self.remote_exception = RemoteException(result)
                self.active_jobs = 0
            self.jobs_cond.notify_all()

    def wait(self):
        """Block until all outstanding jobs have finished.
        """
        with self.jobs_cond:
            while self.active_jobs:
                self.jobs_cond.wait()

            # Raise worker exception on main thread.
            exc = self.remote_exception
            if exc:
                self.remote_exception = None
                raise exc


class ClusterExecutor(concurrent.futures.Executor):
    def __init__(self, host='localhost', port=cw.PORT):
        self.thread = BaseClientThread(self._completion, host, port)
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

        jobid = cw.randid()
        with self.jobs_lock:
            self.futures[jobid] = future
        self.thread.start_job(jobid, func, *args, **kwargs)

        return future

    def shutdown(self, wait=True):
        if wait:
            with self.jobs_lock:
                if self.futures:
                    self.jobs_empty_cond.wait()

        self.thread.stop()
        self.thread.join()


class SlurmExecutor(ClusterExecutor):
    def __init__(self):
        super(SlurmExecutor, self).__init__(cw.slurm_master_host())


def test():
    def square(n):
        return n * n

    with ClusterExecutor() as executor:
        for res in executor.map(square, range(1000)):
            print(res)


if __name__ == '__main__':
    test()
