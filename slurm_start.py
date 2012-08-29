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
        print('master beginning')
        cw.master.Master().run()
        print('master exited')

def start(workers):
    host = subprocess.check_output("hostname").strip()
    print('cluster-workers starting from', host)

    print('starting master thread')
    thread = MasterThread()
    thread.start()
    time.sleep(1)  # Wait for listening socket to be ready.

    print('starting worker job')
    jobid = start_workers(host, workers)
    print('worker job', jobid, 'started')

    try:
        # Block indefinitely.
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        print('stopping slurm job', jobid)
        subprocess.check_output(['scancel', str(jobid)])

if __name__ == '__main__':
    args = sys.argv[1:]
    if args:
        num = int(args.pop())
    else:
        num = DEFAULT_WORKERS
    start(num)
