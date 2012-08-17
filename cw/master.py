import cw
import bluelet

class Master(object):
    def __init__(self):
        self.queued_tasks = []  # (TaskMessage, client connection) pairs
        self.idle_workers = []  # connections
        self.active_tasks = {}  # {jobid: client connection}

    def communicate(self, conn):
        while True:
            # Read a message on the socket.
            print('{} idle workers; {} tasks queued; {} active'.format(
                len(self.idle_workers), len(self.queued_tasks),
                len(self.active_tasks)
            ))
            msg = yield cw._readmsg(conn)
            if msg is None:
                break

            if isinstance(msg, cw.TaskMessage):
                print('got a task')
                self.queued_tasks.append((msg, conn))
            elif isinstance(msg, cw.ResultMessage):
                print('got a result')
                client = self.active_tasks.pop(msg.jobid)
                self.idle_workers.append(conn)
                yield cw._sendmsg(client, msg)
            elif isinstance(msg, cw.WorkerRegisterMessage):
                print('got a worker')
                self.idle_workers.append(conn)
            elif isinstance(msg, cw.WorkerDepartMessage):
                print('lost a worker')
                self.idle_workers.remove(conn)
            else:
                assert False

            # Dispatch as many waiting tasks as we can.
            while self.queued_tasks and self.idle_workers:
                print('dispatching')
                task_message, client = self.queued_tasks.pop(0)
                worker = self.idle_workers.pop(0)
                self.active_tasks[task_message.jobid] = client
                yield cw._sendmsg(worker, task_message)

    def run(self):
        bluelet.run(bluelet.server('', cw.PORT, self.communicate))

if __name__ == '__main__':
    Master().run()
