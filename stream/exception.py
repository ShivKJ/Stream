class StreamClosedException(Exception):
    """
    Exception thrown in case Stream is closed and stream functions
    are invoked.
    """
    pass


class NoElementInStream(Exception):
    """
    This exception is raised when reduce Stream operation is
    called on Stream having no element.
    """
    pass
