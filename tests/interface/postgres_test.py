# -*- coding: utf-8 -*-

from morp.interface import find_environ

try:
    from morp.interface.postgres import Postgres
except ImportError:
    pass

from . import _InterfaceTest, skipIf


@skipIf(
    (
        Postgres is None
        or not any(
            c for c in find_environ(_InterfaceTest.DSN_ENV_NAME)
            if c.interface_class is Postgres
        )
    ),
    "Skipping postgres interface because environment not configured"
)
class InterfaceTest(_InterfaceTest):
    interface_class = Postgres

