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

Documentation is currently sparse. But here's the gist:

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

Using With SLURM
----------------

`SLURM`_ is a cluster management system for Linux. This package contains a few
niceties for running jobs on a SLURM cluster.

The ``slurm.py`` script lets you run SLURM jobs for your master and workers.
Just run ``./slurm.py -n NWORKERS start`` to kick off a master job and a bunch
of worker jobs.

Then, use ``cw.slurm_master_host()`` in your client programs to automatically
find the host of the master to connect to. Pass this to the ``ClientThread``
constructor.

.. _SLURM: https://computing.llnl.gov/linux/slurm/

Author
------

By `Adrian Sampson`_. Code is made available under the `MIT license`_.

.. _MIT license: http://www.opensource.org/licenses/mit-license.php
.. _Adrian Sampson: http://www.cs.washington.edu/homes/asampson/
.. _execnet-futures: https://github.com/sampsyo/execnet-futures/
.. _clusterfutures: https://github.com/sampsyo/clusterfutures/
