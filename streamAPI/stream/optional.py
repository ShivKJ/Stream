from typing import Generic, Union

from streamAPI.utility.Types import Consumer, T, X


class Optional(Generic[T]):
    """
    This class wraps data. This helps avoid processing None element.
    """

    def __init__(self, data: T):
        self.data = data

    def present(self) -> bool:
        """
        Optional(None).present() -> True
        Optional(1).present() -> True

        EMPTY.present() -> False

        :return:
        """

        return self.data is not EMPTY

    def get(self) -> T:
        """
        returns the contained value if this object is not EMPTY,
        else throws ValueError

        :return:
        """

        if not self.present():
            raise ValueError('data is not present')

        return self.data

    def if_present(self, consumer: Consumer[T]):
        """
        processes data only if this object is not EMPTY

        :param consumer:
        :return:
        """

        if self.present():
            consumer(self.data)

    def or_else(self, other: X) -> Union[T, X]:
        """
        returns "other" in case this object is EMPTY else contained Object

        :param other:
        :return:
        """

        return self.data if self.present() else other

    def or_raise(self, exception: Exception) -> T:
        """
        return contained value if this object is not EMPTY else
        raises the "exception"

        :param exception:
        :return:
        """

        if not self.present():
            raise exception

        return self.data

    def __str__(self):
        return 'Optional[' + str(self.data) + ']' if self is not EMPTY else 'EMPTY'

    def __repr__(self):
        return str(self)

    def __eq__(self, other: 'Optional[T]') -> bool:
        return self is other or (other is not EMPTY and self.data == other.data)

    def __hash__(self):
        return hash(self.data) if self is not EMPTY else 0


EMPTY = Optional(None)
