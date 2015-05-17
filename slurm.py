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
        '-d', '--docker-image', dest='docker_image', type=str, default=None,
        help='run workers with in a docker image'
    )
    parser.add_argument(
        '--docker-args', dest='docker_args', type=str, default=None,
        help='arguments to pass to `docker run`'
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
    parser.add_argument(
        '--Xmaster', dest='master_options', metavar='ARG',
        default=[], action='append',
        help='arguments to pass to master job',
    )
    args = parser.parse_args()

    worker_options = args.worker_options
    if args.isolated:
        worker_options.append('--ntasks-per-node=1')

    if args.action == 'start':
        cw.slurm.start(args.nworkers, args.master, args.workers,
              args.master_options, worker_options,
              args.docker_image, args.docker_args)
    elif args.action == 'stop':
        cw.slurm.stop(args.master, args.workers)
    else:
        parser.error('action must be stop or start')


if __name__ == '__main__':
    cli()
