#!/usr/bin/env python

from __future__ import print_function
import re
import os
import tempfile
import subprocess
import sys
import cw.master

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
    command = "sleep 2 ; {} -m cw.worker".format(sys.executable, host)
    name = "cworkers"
    script_lines = [
        "#!/bin/sh",
        "#SBATCH --nodes={} --job-name={}".format(num, name),
        "srun {}".format(command),
    ]
    return sbatch('\n'.join(script_lines))

def main():
    host = subprocess.check_output("hostname").strip()
    print('cluster-workers starting from', host)
    jobid = start_workers(host)
    print('worker job', jobid, 'started')

    try:
        cw.master.Master().run()
    except KeyboardInterrupt:
        pass
    finally:
        print('stopping slurm job', jobid)
        subprocess.check_output(['scancel', str(jobid)])

if __name__ == '__main__':
    main()
