import cw.client


def completion(jobid, output):
    print(u'the square for job {} is {}'.format(jobid, output))


def work(n):
    return n * n


def main():
    # This example uses a context manager to launch the master and
    # workers for this task (as opposed to an explicit `start` and
    # `stop`). You can choose from `cw.mp.allocate`,
    # `cw.slurm.allocate`, and `cw.allocate` to choose automatically.
    with cw.allocate(nworkers=2):

        # A second context manager creates a client for the cluster. You
        # can use this independently of the cluster startup context if
        # you, for example, want to spin up the cluster workers
        # manually and keep them around for several clients.
        with cw.client.ClientThread(completion) as client:

            # Now submit the actual work.
            for i in range(10):
                jobid = cw.randid()
                print(u'submitting job {} to square {}'.format(jobid, i))
                client.submit(jobid, work, i)


if __name__ == '__main__':
    main()
