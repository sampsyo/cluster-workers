from __future__ import print_function
import cw
import bluelet

class Master(object):
    def __init__(self):
        self.queued_tasks = []  # (TaskMessage, client connection) pairs
        self.idle_workers = []  # connections
        self.active_tasks = {}  # {jobid: client connection}
        self.connections = set()  # all connections (client + worker)

    def _show_workers(self):
        print('workers:', len(self.idle_workers) + len(self.active_tasks))

    def communicate(self, conn):
        self.connections.add(conn)

        while True:
            # Read a message on the socket.
            msg = yield cw._readmsg(conn)
            if msg is None:
                break

            if isinstance(msg, cw.TaskMessage):
                self.queued_tasks.append((msg, conn))
            elif isinstance(msg, cw.ResultMessage):
                client = self.active_tasks.pop(msg.jobid)
                self.idle_workers.append(conn)
                if client in self.connections:
                    # Ensure client has not disappeared.
                    yield cw._sendmsg(client, msg)
            elif isinstance(msg, cw.WorkerRegisterMessage):
                self.idle_workers.append(conn)
                self._show_workers()
            elif isinstance(msg, cw.WorkerDepartMessage):
                self.idle_workers.remove(conn)
                self._show_workers()
            else:
                assert False

            # Dispatch as many waiting tasks as we can.
            while self.queued_tasks and self.idle_workers:
                task_message, client = self.queued_tasks.pop(0)
                if client in self.connections:
                    worker = self.idle_workers.pop(0)
                    self.active_tasks[task_message.jobid] = client
                    yield cw._sendmsg(worker, task_message)

        self.connections.remove(conn)

    def run(self):
        bluelet.run(bluelet.server('', cw.PORT, self.communicate))

if __name__ == '__main__':
    Master().run()
