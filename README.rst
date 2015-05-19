cluster-workers
===============

This is a simple client/master/worker system for distributing Python jobs in a
cluster. It is my third attempt (after `clusterfutures`_ and `execnet-futures`_)
to build a comfortable system for running huge, parallel jobs in Python.

Architecture
------------

As with something like Hadoop, there are three distinct machine roles in
cluster-workers. *Workers* run jobs; you typically run one of these per core or
per machine. The *client* produces the jobs; it's your main program that you're
aiming to offload work from. The *master* maintains connections to the workers
and the client; it is responsible for routing jobs and responses between the
other nodes.

It's certainly possible for multiple clients to share the same master, but
fairness enforcement is currently pretty simplistic (FIFO). The distinction
between the master and the client is primarily to avoid needing to spin up new
workers for every new task you want to run. This way, you can allocate nodes to
do your work and leave them running while you run various client programs. You
can add and remove workers at your leisure whether a client is running or not.

The advantage over clusterfutures is that the cluster management infrastructure
is not involved with the inner loop. On a SLURM cluster, for example, each new
call would require a new SLURM job. If the cluster failed to start one job,
clusterfutures had no way of knowing this and would wait forever for the job to
complete. With cluster-workers, SLURM is only involved "offline" and cannot hold
up the actual task execution.

Using
-----

Documentation is currently sparse. But take a look at the ``square.py``
example in the ``examples`` directory for a quick introduction. Here's the
gist of how things work:

* Start a master process with ``python -m cw.master``.
* Start lots of workers with ``python -m cw.worker [HOST]``. Provide the
  hostname of the master (or omit it if the master is on the same host).
* In your client program, start a ``ClientThread``. The constructor takes a
  callback function and the master hostname (the default is again
  ``localhost``). Call the ``submit`` method to send jobs and wait for
  callbacks.

There's also a ``ClusterExecutor`` class that lets you use Python's
`concurrent.futures`_ module as a more convenient way to start jobs.
This may be untenable, however, if your task has a lot of jobs and a lot of
data because of the way futures must persist in memory.

.. _concurrent.futures:
    http://docs.python.org/dev/library/concurrent.futures.html

A Consistent Environment
''''''''''''''''''''''''

The infrastructure tries to make it as seamless as possible to execute code
remotely by replicating your local environment on the remote machine.
Specifically:

* You can send a closure; captured values will be included in the remote
  context (thanks to `the old PiCloud library`_).
* The code is executed on the worker using the same working directory as the
  client. (This assumes the remote machines have a shared filesystem.)
* The Python search path, ``sys.path``, is extended on the worker to match the
  client's. (Again, this assumes a shared filesystem.) This makes it possible
  to, for example, run tasks that use libraries inside of a `virtualenv`_.

.. _virtualenv: https://pypi.python.org/pypi/virtualenv
.. _the old PiCloud library: https://pypi.python.org/pypi/cloud

Using With SLURM
----------------

`SLURM`_ is a cluster management system for Linux. This package contains a few
niceties for running jobs on a SLURM cluster.

The ``slurm.py`` script lets you run SLURM jobs for your master and workers.
Just run ``./slurm.py -n NWORKERS start`` to kick off a master job and a bunch
of worker jobs.

Then, use ``cw.slurm.master_host()`` in your client programs to automatically
find the host of the master to connect to. Pass this to the ``ClientThread``
constructor.

.. _SLURM: https://computing.llnl.gov/linux/slurm/

Using Locally on an SMP
-----------------------

For testing and small jobs, you may want to run a cluster-workers program on a
single multiprocessor machine. The included ``mp.py`` script works like
``slurm.py`` but starts the master and workers on the local machine.

Using with Docker
-----------------

Slurm mode now has the option to run cluster workers as `Docker`_ containers.
Use the following command-line arguments to specify how to run:

* ``--docker-image <image/tag>``: the name of the docker image to use (if
  unspecified, docker will not be invoked)
* ``--docker-args "..."``: arguments to pass to ``docker run``. In particular,
  you must mount the shared filesystem that cluster-workers uses for logging
  output.

The Docker image must have python and cluster-workers installed, and must
specify the python executable as its "entrypoint". For an example, see this
`Dockerfile`_.

.. _Docker: http://www.docker.com
.. _Dockerfile: https://github.com/bholt/vm/blob/master/cluster-worker/Dockerfile

Note that when run in a cluster environment, it may take some time for each
node to pull the specified docker image the first time, so the worker jobs may
take a while to fire up. But subsequent runs should be quite quick.

Author
------

By `Adrian Sampson`_. Code is made available under the `MIT license`_.

.. _MIT license: http://www.opensource.org/licenses/mit-license.php
.. _Adrian Sampson: http://www.cs.washington.edu/homes/asampson/
.. _execnet-futures: https://github.com/sampsyo/execnet-futures/
.. _clusterfutures: https://github.com/sampsyo/clusterfutures/
