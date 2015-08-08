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
    # This command will launch a cluster of workers using the Slurm
    # job manager if it is available, but otherwise will launch workers
    # locally to take advantage of multi-core parallelism.
    #
    # To bypass this auto-detect, use submodule versions: `cw.mp.start()` 
    # to launch workers locally, or `cw.slurm.start()`. Additional 
    # arguments (such as which Slurm partition to use) can be passed to
    # `cw.start()` and they will be passed along if slurm is available.
    cw.start(nworkers=2)

    # Set up the client, connecting to the master host. 
    # If no host is specified (as here), it will will guess which host
    # to use (e.g. if slurm is available, it will use `cw.slurm.master_host()`,
    # otherwise it will default to `localhost` for MP mode.
    client = cw.client.ClientThread(completion)
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
    # This will use the same auto-detection mechanism to determine
    # how to shutdown. To explicitly stop a slurm cluster, for example,
    # use `cw.slurm.stop()`, which will free up the allocated nodes.
    cw.stop()

if __name__ == '__main__':
    main()
