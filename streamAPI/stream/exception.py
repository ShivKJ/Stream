"""
author: Shiv
email: shivkj001@gmail.com
"""


class PipelineClosed(Exception):
    """
    Exception thrown in case PipeLine(for example Stream) is closed.
    """
    pass


class PipelineNOTClosed(Exception):
    """
    Exception thrown in case PipeLine is not closed.
    """
    pass
