import os

from .config import DsnConnection, Connection
from . import decorators
from .interface import get_interface, set_interface, get_interfaces
from . import reflection
from .message import Message
from .exception import InterfaceError


__version__ = '0.3.5'


def configure_environ(dsn_env_name='MORP_DSN'):
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
    if dsn_env_name in os.environ:
        configure(os.environ[dsn_env_name])

    # now try importing 1 -> N dsns
    increment_name = lambda name, num: '{}_{}'.format(name, num)
    dsn_num = 1
    dsn_env_num_name = increment_name(dsn_env_name, dsn_num)
    while dsn_env_num_name in os.environ:
        configure(os.environ[dsn_env_num_name])
        dsn_num += 1
        dsn_env_num_name = increment_name(dsn_env_name, dsn_num)


def configure(dsn):
    """
    configure an interface to be used to send messages to a backend

    you use this function to configure an Interface using a dsn, then you can get
    that interface using the get_interface() method

    dsn -- string -- a properly formatted prom dsn, see DsnConnection for how to format the dsn
    """
    #global interfaces

    c = DsnConnection(dsn)
    if c.name in get_interfaces():
        raise ValueError('a connection named "{}" has already been configured'.format(c.name))

    interface_class = reflection.get_class(c.interface_name)
    i = interface_class(c)
    set_interface(i, c.name)
    return i


configure_environ()

