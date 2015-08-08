import cw.client
import cw.slurm
import cw.mp


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

    # If you wish to start a worker cluster just for this script, you
    # can do so with a command like this. See the slurm module for
    # more info on the available options.
    #
    # Note: If you are running multiple cluster-workers scripts, be
    # careful with this because only one set of master/workers can be
    # running at a time.
    #
    # This example shows how to launch workers locally on a multi-
    # processor. To instead launch on a cluster using slurm, change
    # the command below to `cw.slurm.start(nworkers=2)`
    cw.mp.start(nworkers=2)

    # Set up the client, connecting to the master host. Replace the
    # second argument with `cw.slurm.master_host()` if using slurm to 
    # launch the workers.
    client = cw.client.ClientThread(completion, 'localhost')
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

    # If you started the worker cluster programmatically above, you
    # can safely shut down the cluster workers here.
    # 
    # If using slurm, change to `cw.slurm.stop()`, which will free
    # up the allocated nodes.
    cw.mp.stop()

if __name__ == '__main__':
    main()
