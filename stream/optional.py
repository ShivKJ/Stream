from typing import TypeVar, Generic

T = TypeVar('T')


class Optional(Generic[T]):
    """
    This class wraps data. This helps avoid processing None element.
    """

    def __init__(self, data: T):
        self.data = data

    def present(self):
        return self.data is not None

    def get(self):
        if not self.present():
            raise ValueError('data is None')

        return self.data

    def if_present(self, consumer):
        if self.present():
            consumer(self.data)

    def or_else(self, other):
        return self.data if self.present() else other

    def or_raise(self, exception: Exception):
        if not self.present():
            raise exception

        return self.data

    def __str__(self):
        return str(self.data)


EMPTY = Optional(None)
