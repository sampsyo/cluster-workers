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
import getpass

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
    """Given a job name, return the ID of a job belonging to this user
    matching that name or None if not found.
    """
    cur_user = getpass.getuser()
    for jobid, name, user, nodelist in cw.slurm_jobinfo():
        if name == jobname and user == cur_user:
            return jobid

def start_workers(num=2, options=()):
    command = "{} -m cw.worker --slurm".format(sys.executable)
    options = ['--ntasks={}'.format(num)] + options
    return startjob(command, cw.JOB_WORKERS, options)

def start_master():
    """Start a job for the master process. Return the Slurm job ID.
    """
    command = "{} -m cw.master".format(sys.executable)
    return startjob(command, cw.JOB_MASTER)

def start(nworkers, master=True, workers=True, worker_options=()):
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
        jobid = start_workers(nworkers, worker_options)
        print('worker job', jobid, 'started')

def stop(master=True, workers=True):
    # Workers.
    worker_jobid = get_jobid(cw.JOB_WORKERS)
    if worker_jobid:
        print('stopping workers')
        scancel(worker_jobid)
        time.sleep(5)

    # Master.
    master_jobid = get_jobid(cw.JOB_MASTER)
    if master_jobid:
        print('stopping master')
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
    parser.add_argument(
        '-i', '--isolated', dest='isolated', action='store_true',
        default=False, help='only one worker per node'
    )
    parser.add_argument(
        '--Xworkers', dest='worker_options', metavar='ARG',
        default=[], action='append',
        help='arguments to pass to worker job',
    )
    args = parser.parse_args()

    worker_options = args.worker_options
    if args.isolated:
        worker_options.append('--ntasks-per-node=1')

    if args.action == 'start':
        start(args.nworkers, args.master, args.workers, worker_options)
    elif args.action == 'stop':
        stop(args.master, args.workers)
    else:
        parser.error('action must be stop or start')

if __name__ == '__main__':
    cli()
