import cw.client
import cw.mp


def completion(jobid, output):
    print(u'the square for job {} is {}'.format(jobid, output))


def work(n):
    return n * n


def main():
    # This example uses a context manager to launch the master and
    # workers for this task (as opposed to an explicit `start` and
    # `stop`). You can choose from `cw.mp.allocate`,
    # `cw.slurm.allocate`, and `cw.allocate` to choose automatically.
    with cw.mp.allocate(nworkers=2):
        client = cw.client.ClientThread(completion)
        client.start()

        # Submit a bunch of work.
        for i in range(10):
            jobid = cw.randid()
            print(u'submitting job {} to square {}'.format(jobid, i))
            client.submit(jobid, work, i)

        client.wait()


if __name__ == '__main__':
    main()
