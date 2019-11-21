# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import os

import dsnparse

from .compat import *
from .config import DsnConnection, Connection
from . import decorators
from .interface import get_interface, set_interface, get_interfaces
from .message import Message
from .exception import InterfaceError


__version__ = '2.0.0'


def configure_environ(dsn_env_name='MORP_DSN', connection_class=DsnConnection):
    """
    configure interfaces based on environment variables

    by default, when morp is imported, it will look for MORP_DSN, and MORP_DSN_N (where
    N is 1 through infinity) in the environment, if it finds them, it will assume they
    are dsn urls that morp understands and will configure connections with them. If you
    don't want this behavior (ie, you want to configure morp manually) then just make sure
    you don't have any environment variables with matching names

    The num checks (eg MORP_DSN_1, MORP_DSN_2) go in order, so you can't do MORP_DSN_1, MORP_DSN_3,
    because it will fail on _2 and move on, so make sure your num dsns are in order (eg, 1, 2, 3, ...)

    dsn_env_name -- string -- the name of the environment variables prefix
    """
    cs = dsnparse.parse_environs(dsn_env_name, parse_class=connection_class)
    for c in cs:
        set_interface(c.interface, c.name)


def configure(dsn, connection_class=DsnConnection):
    """
    configure an interface to be used to send messages to a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    dsn -- string -- a properly formatted prom dsn, see DsnConnection for how to format the dsn
    """
    #global interfaces
    c = dsnparse.parse(dsn, parse_class=connection_class)
    set_interface(c.interface, c.name)


configure_environ()

