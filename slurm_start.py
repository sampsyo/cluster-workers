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

def nodelist(jobid):
    """Given a Slurm job ID, get the nodes that are running it.
    """
    return subprocess.check_output(
        ['squeue', '-j', str(jobid), '-o', '%N', '-h']
    )

def start_workers(num=2):
    command = "{} -m cw.worker --slurm".format(sys.executable)
    name = "cworkers"
    script_lines = [
        "#!/bin/sh",
        "#SBATCH --nodes={} --job-name={}".format(num, name),
        "srun {}".format(command),
    ]
    return sbatch('\n'.join(script_lines))

def start_master():
    """Start a job for the master process. Return the Slurm job ID.
    """
    command = "{} -m cw.master".format(sys.executable)
    script_lines = [
        "#!/bin/sh",
        "#SBATCH --job-name=cmaster",
        "srun {}".format(command),
    ]
    return sbatch('\n'.join(script_lines))

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

def cli():
    parser = argparse.ArgumentParser(
        description='start Python workers on a Slurm cluster'
    )
    parser.add_argument(
        'nworkers', metavar='N', type=int, nargs='?', default=DEFAULT_WORKERS,
        help='number of workers to start (default {})'.format(DEFAULT_WORKERS)
    )
    parser.add_argument(
        '-M', dest='master', action='store_false', default=True,
        help='do not start master'
    )
    parser.add_argument(
        '-W', dest='workers', action='store_false', default=True,
        help='do not start workers'
    )
    args = parser.parse_args()
    start(args.nworkers, args.master, args.workers)

if __name__ == '__main__':
    cli()
