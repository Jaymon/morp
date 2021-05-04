#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import sys
import os
import logging

from captain import exit as console, arg
from morp.compat import *
from morp import Message


logger = logging.getLogger(__name__)


@arg('--count', "-c", dest='count', type=int, default=0, help='How many messages to consume')
@arg('--dir', '--directory', '-d', dest='basepath', default=os.getcwd(), help='The base directory')
@arg('names', metavar='NAME', nargs='?', default=Message.get_name())
def main_recv(count, basepath, names):
    # we want to make sure the directory can be imported from since chances are
    # the message classes live in that directory
    basepath = os.path.abspath(os.path.expanduser(String(basepath)))
    sys.path.append(basepath)

    Message.name = args.name
    logger.info("Handling {} messages".format(Message.get_name()))
    Message.handle(count=args.count)


@arg('names', metavar='NAME', nargs='?', default=Message.get_name())
def main_send(names, **kwargs):
    pout.v(kwargs)




# def console():
#     parser = argparse.ArgumentParser(description='Consume Morp messages')
#     parser.add_argument("--version", "-V", action='version', version="%(prog)s {}".format(__version__))
#     parser.add_argument('--count', "-c", dest='count', type=int, default=0, help='How many messages to consume')
#     parser.add_argument('--dir', '--directory', '-d', dest='directory', default=os.getcwd(), help='The base directory')
#     parser.add_argument('name', metavar='NAME', nargs='?', default=Message.get_name())
#     args = parser.parse_args()
# 
#     # we want to make sure the directory can be imported from since chances are
#     # the message classes live in that directory
#     basepath = os.path.abspath(os.path.expanduser(String(args.directory)))
#     sys.path.append(basepath)
# 
#     Message.name = args.name
#     logger.info("Handling {} messages".format(Message.get_name()))
#     Message.handle(count=args.count)
#     sys.exit(0)


if __name__ == "__main__":
    # allow both imports of this module, for entry_points, and also running this module using python -m pyt
    console(__name__)

