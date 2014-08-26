
class Error(Exception):
    """error wrapper"""
    def __init__(self, e, exc_info=None):
        self.e = e
        self.exc_info = exc_info
        super(Error, self).__init__(str(e))


class InterfaceError(Error):
    """specifically for wrapping interface errors"""
    pass

