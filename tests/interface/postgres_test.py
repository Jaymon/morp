# -*- coding: utf-8 -*-

from morp.interface.postgres import Postgres

from . import _InterfaceTest


class InterfaceTest(_InterfaceTest):
    interface_class = Postgres

