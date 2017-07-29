#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import sys
import argparse
import os
import logging
import logging.config

from morp import Message, __version__


def configure_logging(val):
    """see --quiet flag help for what this does

    This was ripped from https://github.com/Jaymon/bang/blob/master/bang/__main__.py#L113"""

    if val.startswith("-"):
        # if we had a subtract, then just remove those from being suppressed
        # so -E would only show errors
        levels = val[1:]
    else:
        levels = "".join(set("DIWEC") - set(val.upper()))


    class LevelFilter(object):
        def __init__(self, levels):
            self.levels = levels.upper()
            self.__level = logging.NOTSET
        def filter(self, logRecord):
            return logRecord.levelname[0].upper() in self.levels

    try:
        # https://docs.python.org/2/library/logging.html
        # https://docs.python.org/2/library/logging.config.html#logging-config-dictschema
        # https://docs.python.org/2/howto/logging.html
        # http://stackoverflow.com/questions/8162419/python-logging-specific-level-only
        d = {
            'version': 1,
            'formatters': {
                'basic': {
                    #'format': '[%(levelname).1s|%(filename)s:%(lineno)s] %(message)s',
                    'format': '[%(levelname).1s] %(message)s',
                },
                'message': {
                    'format': '%(message)s'
                }
            },
            'handlers': {
                'stdout': {
                    'level': 'NOTSET',
                    'class': 'logging.StreamHandler',
                    'formatter': 'basic',
                    'filters': ['stdout', 'user'],
                    'stream': 'ext://sys.stdout'
                },
                'stderr': {
                    'level': 'WARNING',
                    'class': 'logging.StreamHandler',
                    'formatter': 'basic',
                    'filters': ['stderr', 'user'],
                    'stream': 'ext://sys.stderr'
                },
            },
            'filters': {
                'stdout': {
                    '()': LevelFilter,
                    'levels': 'DI',
                },
                'stderr': {
                    '()': LevelFilter,
                    'levels': 'WEC',
                },
                'user': {
                    '()': LevelFilter,
                    'levels': levels,
                },
            },
            'root': {
                'level': 'NOTSET',
                'handlers': ['stdout', 'stderr'],
            },
            'loggers': {
                'botocore': {
                    'level': 'WARNING',
                },
                'boto3': {
                    'level': 'WARNING',
                },
            },
            'incremental': False,
            'disable_existing_loggers': False,
        }
        logging.config.dictConfig(d)

    except Exception as e:
        raise

    return val


def console():
    parser = argparse.ArgumentParser(description='Consume Morp messages')
    parser.add_argument("--version", "-V", action='version', version="%(prog)s {}".format(__version__))
    parser.add_argument('--count', "-c", dest='count', type=int, default=0, help='How many messages to consume')
    parser.add_argument('--dir', '--directory', '-d', dest='directory', default=os.getcwd(), help='The base directory')
    parser.add_argument('name', metavar='NAME', nargs='?', default=Message.get_name())
    parser.add_argument(
        '--quiet',
        nargs='?',
        const='DIWEC',
        default='',
        type=configure_logging,
        help=''.join([
            'Selectively turn off [D]ebug, [I]nfo, [W]arning, [E]rror, or [C]ritical, ',
            '(--quiet=DI means suppress Debug and Info), ',
            'use - to invert (--quiet=-E means suppress everything but Error)',
        ])
    )
    args = parser.parse_args()

    # we want to make sure the directory can be imported from since chances are
    # the message classes live in that directory
    basepath = os.path.abspath(os.path.expanduser(str(args.directory)))
    sys.path.append(basepath)

    Message.name = args.name
    Message.handle(count=args.count)
    sys.exit(0)


if __name__ == "__main__":
    # allow both imports of this module, for entry_points, and also running this module using python -m pyt
    console()

