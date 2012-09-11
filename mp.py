#!/usr/bin/env python
from __future__ import print_function
import argparse
import subprocess
import sys
import time

DEFAULT_WORKERS = 8

def ps():
    """Get a list of running processes owned by the current user. Generate
    a list of (pid, args) pairs.
    """
    output = subprocess.check_output(
        ['ps', '-a', '-o', 'pid args']
    )
    lines = output.strip().split('\n')
    for row in lines[1:]:  # Skip header row.
        pid, args = row.split(None, 1)
        yield int(pid), args

def pids_for(argpat):
    """Generate the pids of processes whose arguments contain a given
    string (case-insensitive).
    """
    for pid, args in ps():
        if argpat.lower() in args.lower():
            yield pid

def kill(pids):
    """Kill processes by pid."""
    subprocess.check_call(['kill'] + [str(p) for p in pids])

def start(nworkers, master=True, workers=True):
    if master:
        print('starting master')
        proc = subprocess.Popen([sys.executable, '-m', 'cw.master'])
        print('master pid is {}'.format(proc.pid))
        time.sleep(1)

    if workers:
        print('starting {} workers'.format(nworkers))
        for i in range(nworkers):
            subprocess.Popen([sys.executable, '-m', 'cw.worker'])
        print('workers started')

def stop(master=True, workers=True):
    if workers:
        worker_pids = list(pids_for('python -m cw.worker'))
        if worker_pids:
            print('killing {} workers'.format(len(worker_pids)))
            kill(worker_pids)

    if master:
        master_pids = list(pids_for('python -m cw.master'))
        if master_pids:
            print('killing {} master'.format(len(master_pids)))
            kill(master_pids)

def cli():
    parser = argparse.ArgumentParser(
        description='start Python workers on a multiprocessor machine'
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
