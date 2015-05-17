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


JOB_MASTER ='cmaster'
JOB_WORKERS = 'cworkers'


def _jobinfo():
    """Uses "squeue" to generate a list of job information tuples. The
    tuples are of the form (jobid, jobname, username, nodelist).
    """
    joblist = subprocess.check_output(
        ['squeue', '-o', '%i %j %u %N', '-h']
    ).strip()
    if not joblist:
        return
    for line in joblist.split('\n'):
        jobid, name, user, nodelist = line.split(' ', 3)
        yield int(jobid), name, user, nodelist


def master_host():
    cur_user = getpass.getuser()
    for jobid, name, user, nodelist in _jobinfo():
        if name == JOB_MASTER and user == cur_user:
            assert '[' not in nodelist
            return nodelist
    assert False, 'no master job found'

def _sbatch(job):
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


def _startjob(command, name=None, options=[]):
    """Start a Slurm job and return the job ID."""
    if name:
        options.append('--job-name={}'.format(name))
        options.append('--output={}.out'.format(name))
        options.append('--error={}.out'.format(name))

    script_lines = ["#!/bin/sh"]
    if options:
        script_lines.append('#SBATCH ' + ' '.join(options))
    script_lines.append('srun ' + command)

    return _sbatch('\n'.join(script_lines))


def _scancel(jobid, signal='INT'):
    """Cancel a Slurm job given its ID.
    """
    subprocess.check_call(
        ['scancel', '-s', signal, str(jobid)]
    )


def _get_jobid(jobname):
    """Given a job name, return the ID of a job belonging to this user
    matching that name or None if not found.
    """
    cur_user = getpass.getuser()
    for jobid, name, user, nodelist in _jobinfo():
        if name == jobname and user == cur_user:
            return jobid


def _start_workers(num=2, options=[], docker_image=None, docker_args=""):
    if docker_image:
        command = "docker run -i --rm --net=host {} {} -m cw.worker {}".format(
            docker_args, docker_image, master_host()
        )
    else:
        command = "{} -m cw.worker --slurm".format(sys.executable)
    options = ['--ntasks={}'.format(num)] + options
    return _startjob(command, JOB_WORKERS, options)


def _start_master(options=[]):
    """Start a job for the master process. Return the Slurm job ID.
    """
    command = "{} -m cw.master".format(sys.executable)
    return _startjob(command, JOB_MASTER, options)


def start(nworkers, master=True, workers=True, master_options=[],
          worker_options=[], docker_image=None, docker_args=""):
    """Start up a cluster of workers using Slurm. Note that only one
    cluster should be in operation at a given time (only one 'cmaster').
    If no docker_image is specified, docker will not be used.
    """
    # Master.
    if master:
        print('starting master')
        jobid = _start_master(master_options)
        print('master job', jobid, 'started')
        time.sleep(5)
        print('master running on', master_host())

    # Workers.
    if workers:
        print('starting {} workers'.format(nworkers))
        jobid = _start_workers(nworkers, worker_options,
                              docker_image, docker_args)
        print('worker job', jobid, 'started')


def stop(master=True, workers=True):
    """Stop the running of a cluster (shut down cmaster and all cworkers).
    """
    # Workers.
    worker_jobid = _get_jobid(JOB_WORKERS)
    if worker_jobid:
        print('stopping workers')
        _scancel(worker_jobid)
        time.sleep(5)

    # Master.
    master_jobid = _get_jobid(JOB_MASTER)
    if master_jobid:
        print('stopping master')
        _scancel(master_jobid)
