# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import
import logging
import sys
from contextlib import contextmanager
import base64
import datetime

from cryptography.fernet import Fernet
import dsnparse

from ..compat import *
from ..config import DsnConnection
from ..exception import InterfaceError


logger = logging.getLogger(__name__)


interfaces = {}
"""holds all configured interfaces"""


def get_interfaces():
    global interfaces
    if not interfaces:
        configure_environ()
    return interfaces


def get_interface(connection_name=""):
    """get the configured interface that corresponds to connection_name"""
    global interfaces
    if not interfaces:
        configure_environ()
    return interfaces[connection_name]


def set_interface(interface, connection_name=""):
    """bind an .interface.Interface() instance to connection_name"""
    global interfaces
    interfaces[connection_name] = interface


def find_environ(dsn_env_name='MORP_DSN', connection_class=DsnConnection):
    """Returns Connection instances found in the environment

    configure interfaces based on environment variables

    by default, when morp is imported, it will look for MORP_DSN, and MORP_DSN_N (where
    N is 1 through infinity) in the environment, if it finds them, it will assume they
    are dsn urls that morp understands and will configure connections with them. If you
    don't want this behavior (ie, you want to configure morp manually) then just make sure
    you don't have any environment variables with matching names

    The num checks (eg MORP_DSN_1, MORP_DSN_2) go in order, so you can't do MORP_DSN_1, MORP_DSN_3,
    because it will fail on _2 and move on, so make sure your num dsns are in order (eg, 1, 2, 3, ...)

    :param dsn_env_name: str, the environment variable name to find the DSN, this
        will aslo check for values of <dsn_env_name>_N where N is 1 -> N, so you
        can configure multiple DSNs in the environment and this will pick them all
        up
    :param connection_class: Connection, the class that will receive the dsn values
    :returns: generator<connection_class>
    """
    return dsnparse.parse_environs(dsn_env_name, parse_class=connection_class)


def configure_environ():
    """auto hook to configure the environment"""
    for c in find_environ():
        inter = c.interface
        set_interface(c.interface, c.name)


def configure(dsn, connection_class=DsnConnection):
    """
    configure an interface to be used to send messages to a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    :param dsn: string, a properly formatted morp dsn, see DsnConnection for how to format the dsn
    """
    #global interfaces
    c = dsnparse.parse(dsn, parse_class=connection_class)
    set_interface(c.interface, c.name)

