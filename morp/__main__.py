#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import sys
import argparse
import os
import logging
import logging.config

from morp.compat import *
from morp import Message, __version__


logger = logging.getLogger(__name__)


class QuietAction(argparse.Action):

    OPTIONS = "DIWEC"

    DEST = "quiet"

    HELP_QUIET = ''.join([
        'Selectively turn off [D]ebug, [I]nfo, [W]arning, [E]rror, or [C]ritical, ',
        '(--quiet=DI means suppress Debug and Info), ',
        'use - to invert (--quiet=-EW means suppress everything but Error and warning)',
    ])

    HELP_Q_LOWER = ''.join([
        'Turn off [D]ebug (-q), [I]nfo (-qq), [W]arning (-qqq), [E]rror (-qqqq), ',
        'and [C]ritical (-qqqqq)',
    ])

    HELP_Q_UPPER = 'Turn off all logging: [D]ebug, [I]nfo, [W]arning, [E]rror, and [C]ritical'

    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        kwargs["required"] = False
        if "-Q" in option_strings:
            kwargs["default"] = False
            kwargs["const"] = True
            kwargs["nargs"] = 0
        elif "-q" in option_strings:
            kwargs["required"] = False
            kwargs["nargs"] = 0
            kwargs["default"] = None

        super(QuietAction, self).__init__(option_strings, self.DEST, **kwargs)

    def __call__(self, parser, namespace, values, option_string=""):
        #pout.v(parser, namespace, values, option_string)

        if option_string.startswith("--"):
            self.configure_logging(values)

        elif option_string.startswith("-q"):
            count = len(values) + 1
            val = self.OPTIONS[0:count]
            self.configure_logging(val)

        elif option_string.startswith("-Q"):
            self.configure_logging("")

        else:
            raise ValueError()

        setattr(namespace, self.dest, self)

    def call_q(self, count):
        val = self.OPTIONS[0:count]
        self.configure_logging(val)

    def configure_logging(self, val):
        """see --quiet flag help for what this does

        This was ripped from https://github.com/Jaymon/bang/blob/master/bang/__main__.py#L113"""

        if val.startswith("-"):
            # if we had a subtract, then just remove those from being suppressed
            # so -E would only show errors
            levels = val[1:]
        else:
            levels = "".join(set(self.OPTIONS) - set(val.upper()))

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
        #nargs='?',
        #const='DIWEC',
        #default='',
        action=QuietAction,
        #type=configure_quiet_logging,
        help=QuietAction.HELP_QUIET,
    )
    parser.add_argument(
        "-q",
        action=QuietAction,
        help=QuietAction.HELP_Q_LOWER,
    )
    parser.add_argument(
        "-Q",
        action=QuietAction,
        help=QuietAction.HELP_Q_UPPER,
    )

    args = parser.parse_args()

    # we want to make sure the directory can be imported from since chances are
    # the message classes live in that directory
    basepath = os.path.abspath(os.path.expanduser(String(args.directory)))
    sys.path.append(basepath)

    Message.name = args.name
    logger.info("Handling {} messages".format(Message.get_name()))
    Message.handle(count=args.count)
    sys.exit(0)


if __name__ == "__main__":
    # allow both imports of this module, for entry_points, and also running this module using python -m pyt
    console()

