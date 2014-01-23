import cw.client


def completion(jobid, output):
    """A callback function tat is invoked whenever a cluster job
    completes. Two arguments: the job ID (provided with submission) and
    the return value of the job's function invocation.
    """
    print(u'the square for job {} is {}'.format(jobid, output))


def work(n):
    """Square the argument! So much work to be done; better parallelize
    it.
    """
    return n * n


def main():
    # Set up the client, connecting to the mast host. Replace the second
    # argument below with 'localhost' if you want to connect to a local
    # (i.e., development) cluster.
    client = cw.client.ClientThread(completion, cw.slurm_master_host())
    client.start()

    # Submit a bunch of work.
    for i in range(10):
        # cw.randid() is a convenient utility function for generating
        # unique job IDs in case you don't already have natural IDs.
        jobid = cw.randid()
        print(u'submitting job {} to square {}'.format(jobid, i))
        client.submit(jobid, work, i)

    # Wait for all of the jobs to finish. This helps ensure that we see
    # the results of all of our work before shutting down the
    # interpreter.
    client.wait()


if __name__ == '__main__':
    main()
