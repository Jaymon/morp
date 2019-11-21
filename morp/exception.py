# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import


class Error(Exception):
    """error wrapper"""
    def __init__(self, e, exc_info=None):
        self.e = e
        self.exc_info = exc_info
        super(Error, self).__init__(str(e))


class InterfaceError(Error):
    """specifically for wrapping interface errors"""
    pass


class ReleaseMessage(InterfaceError):
    """Can be raised anytime in a recv() with block or target() method to get the
    message to be released

    :param delay_seconds: int, how long until message is released to be processed
        again, the visibility timeout 
    """
    def __init__(self, delay_seconds=0):
        super(ReleaseMessage, self).__init__("")
        self.delay_seconds = delay_seconds


class AckMessage(InterfaceError):
    """Can be raised anytime in a recv() with block or target() method to get the
    message to be acknowledged"""
    def __init__(self):
        super(AckMessage, self).__init__("")

