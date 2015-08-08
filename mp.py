#!/usr/bin/env python
from __future__ import print_function
import argparse
import cw.mp

DEFAULT_WORKERS = 8


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
        cw.mp.start(args.nworkers, args.master, args.workers)
    elif args.action == 'stop':
        cw.mp.stop(args.master, args.workers)
    else:
        parser.error('action must be stop or start')


if __name__ == '__main__':
    cli()
