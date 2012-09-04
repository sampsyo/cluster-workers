#!/usr/bin/env python
from __future__ import print_function
import re
import os
import tempfile
import subprocess
import sys
import cw.master
import threading
import time
import argparse

DEFAULT_WORKERS = 16

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

def start_workers(host, num=2):
    command = "{} -m cw.worker {}".format(sys.executable, host)
    name = "cworkers"
    script_lines = [
        "#!/bin/sh",
        "#SBATCH --nodes={} --job-name={}".format(num, name),
        "srun {}".format(command),
    ]
    return sbatch('\n'.join(script_lines))

class MasterThread(threading.Thread):
    def __init__(self):
        super(MasterThread, self).__init__()
        self.daemon = True

    def run(self):
        cw.master.Master().run()

def start(workers, host=None, master=False):
    if host is None:
        host = subprocess.check_output("hostname").strip()
    print('starting {} workers for master {}'.format(workers, host))

    if master:
        print('starting master thread')
        thread = MasterThread()
        thread.start()
        time.sleep(1)  # Wait for listening socket to be ready.

    print('starting worker job')
    jobid = start_workers(host, workers)
    print('worker job', jobid, 'started')

    if master:
        try:
            # Block indefinitely.
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass
        finally:
            print('stopping slurm job', jobid)
            subprocess.check_output(['scancel', '--signal=INT', str(jobid)])
            time.sleep(1)

def cli():
    parser = argparse.ArgumentParser(
        description='start Python workers on a Slurm cluster'
    )
    parser.add_argument(
        'workers', metavar='N', type=int, nargs='?', default=DEFAULT_WORKERS,
        help='number of workers to start (default {})'.format(DEFAULT_WORKERS)
    )
    parser.add_argument(
        '-m', dest='host', metavar='HOST', type=str,
        help='master hostname (default this host)'
    )
    parser.add_argument(
        '-M', dest='master', action='store_true',
        help='block and run master too'
    )
    args = parser.parse_args()
    start(args.workers, args.host, args.master)

if __name__ == '__main__':
    cli()
