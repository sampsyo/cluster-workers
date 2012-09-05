#!/usr/bin/env python
from __future__ import print_function
import re
import os
import tempfile
import subprocess
import sys
import time
import argparse
import cw

DEFAULT_WORKERS = 32

def sbatch(job):
    """Submits a Slurm job represented as a sbatch script string. Returns
    the job ID.
    """
    jobfile = tempfile.NamedTemporaryFile(delete=False)
    jobfile.write(job)
    jobfile.close()
    try:
        out = subprocess.check_output(['sbatch', jobfile.name])
    finally:
        os.unlink(jobfile.name)

    jobid = re.search(r'job (\d+)', out).group(1)
    return int(jobid)

def startjob(command, name=None, options=()):
    """Start a Slurm job and return the job ID."""
    options = list(options)
    if name:
        options.append('--job-name={}'.format(name))
        options.append('--output={}.out'.format(name))
        options.append('--error={}.out'.format(name))

    script_lines = ["#!/bin/sh"]
    if options:
        script_lines.append('#SBATCH ' + ' '.join(options))
    script_lines.append('srun ' + command)

    return sbatch('\n'.join(script_lines))

def scancel(jobid, signal='INT'):
    """Cancel a Slurm job given its ID.
    """
    subprocess.check_call(
        ['scancel', '-s', signal, str(jobid)]
    )

def get_jobid(jobname):
    """Given a job name, return its ID or None if not found.
    """
    for jobid, name, nodelist in cw.slurm_jobinfo():
        if name == jobname:
            return jobid

def start_workers(num=2):
    command = "{} -m cw.worker --slurm".format(sys.executable)
    options = ['--ntasks={}'.format(num)]
    return startjob(command, cw.JOB_WORKERS, options)

def start_master():
    """Start a job for the master process. Return the Slurm job ID.
    """
    command = "{} -m cw.master".format(sys.executable)
    return startjob(command, cw.JOB_MASTER)

def start(nworkers, master=True, workers=True):
    # Master.
    if master:
        print('starting master')
        jobid = start_master()
        print('master job', jobid, 'started')
        time.sleep(5)
        print('master running on', cw.slurm_master_host())

    # Workers.
    if workers:
        print('starting {} workers'.format(nworkers))
        jobid = start_workers(nworkers)
        print('worker job', jobid, 'started')

def stop(master=True, workers=True):
    # Workers.
    worker_jobid = get_jobid(cw.JOB_MASTER)
    if worker_jobid:
        print('stopping workers')
        scancel(worker_jobid)
        time.sleep(5)

    # Master.
    master_jobid = get_jobid(cw.JOB_MASTER)
    if master_jobid:
        print('stopping workers')
        scancel(master_jobid)

def cli():
    parser = argparse.ArgumentParser(
        description='start Python workers on a Slurm cluster'
    )
    parser.add_argument(
        'action', metavar='start|stop', help='action'
    )
    parser.add_argument(
        '-n', dest='nworkers', metavar='N', type=int, default=DEFAULT_WORKERS,
        help='number of workers to start (default {})'.format(DEFAULT_WORKERS)
    )
    parser.add_argument(
        '-M', dest='master', action='store_false', default=True,
        help='do not start/stop master'
    )
    parser.add_argument(
        '-W', dest='workers', action='store_false', default=True,
        help='do not start/stop workers'
    )
    args = parser.parse_args()

    if args.action == 'start':
        start(args.nworkers, args.master, args.workers)
    elif args.action == 'stop':
        stop(args.master, args.workers)
    else:
        parser.error('action must be stop or start')

if __name__ == '__main__':
    cli()
