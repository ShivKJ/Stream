"""
author: Shiv
email: shivkj001@gmail.com
"""

from abc import ABC, abstractmethod
from collections import deque
from typing import Callable, Deque, Iterable

from streamAPI.stream.decos import check_pipeline, close_pipeline
from streamAPI.stream.exception import PipelineNOTClosed
from streamAPI.stream.optional import EMPTY, Optional
from streamAPI.utility.Types import Filter, Function, X
from streamAPI.utility.utils import always_true, get_functions_clazz


class Supplier(Iterable[X]):
    """
    This class provide a wrapper around a callable function.

    Example:
        from random import random

        supplier = Supplier(random)

        print(next(supplier))
        print(next(supplier))
        print(next(supplier))

        for idx, x in enumerate(supplier):
            print(x)

            if idx == 10:
                break

    """

    def __init__(self, func: Callable[[], X]):
        super().__init__()

        self._func = func

    def __iter__(self):
        while True:
            yield next(self)

    def __next__(self):
        return self._func()


class AbstractCondition(ABC):
    @abstractmethod
    def apply(self, e):
        """
        Transforming the element depending on condition specified.

        :param e:
        :return:
        """

    def __call__(self, e):
        return self.apply(e)


class _IfThen(AbstractCondition):
    """
    Objects of this class will be chained together in ChainedCondition
    class object.

    def transform(x):
        if predicate(x):
            return Optional(func(x))
        else:
            return EMPTY


    That is equivalent to: IfThen(predicate, func)
    """

    def __init__(self, if_: Filter, then: Function):
        super().__init__()

        self._if = if_
        self._then = then

    def apply(self, e) -> Optional:
        """
        If "if_" condition fails then EMPTY is returned otherwise
        "then" function is used to transformed the element.

        :param e:
        :return:
        """

        return Optional(self._then(e)) if self._if(e) else EMPTY


class Closable:
    def __init__(self):
        super().__init__()

        self._closed = False

    @property
    def closed(self):
        """
        Gets current state of pipeline.
        :return:
        """

        return self._closed

    @closed.setter
    def closed(self, state: bool):
        """
        Updates state of pipeline.

        :param closed:
        :return:
        """

        self._closed = state


class ChainedCondition(Closable, AbstractCondition):
    """
    This class will help Stream in transforming elements on the basis
    of conditions.

    def transform(x):
        if predicate1(x):
            return f1(x)
        elif predicate2(x):
            return f2(x)
        .
        .
        .
        else:
            return fn(x)

    That is equivalent to:

    ChainedCondition().if_then(predicate1,f1).if_then(predicate2,f2). ... .otherwise(fn)

    Note that before applying element, chainedCondition must be closed; ChainedCondition
    can be closed by invoking "otherwise" or "done" method.

    If "done" method has been chosen to close the Pipeline and if no condition
    defined by ChainedCondition object returns True then element itself is returned.

    """

    def __init__(self, name=None):
        super().__init__()

        self._conditions: Deque[_IfThen] = deque()
        self._name = name
        self._else_called = False

    @classmethod
    def if_else(cls, if_: Filter, then: Function, else_: Function) -> 'ChainedCondition':
        """
        Creates a ChainedCondition.

        If "if_" returns True on an element then ChainedCondition
        will return "then(element)" otherwise else_(element) will be returned
        on invoking "apply" method on this class object.

        :param if_:
        :param then:
        :param else_:
        :return:
        """

        return cls().if_then(if_, then).otherwise(else_)

    @check_pipeline
    def if_then(self, if_: Filter, then: Function):
        """
        Creates _IfThen object from given "if_" and "then".

        :param if_:
        :param then:
        :return:
        """

        self._conditions.append(_IfThen(if_, then))
        return self

    @close_pipeline
    @check_pipeline
    def otherwise(self, else_: Function):
        """
        Adds "else" condition to ChainedCondition object.
        After this method, pipeline will be closed.

        It is required that "if" condition must be specified
        before invoking this method.

        :param else_:
        :return:
        """

        if not self._conditions:
            raise AttributeError("No 'if' condition added.")

        self._else_called = True

        return self.if_then(always_true, else_)

    @close_pipeline
    @check_pipeline
    def done(self):
        """
        closes the ChainedCondition pipeline.
        :return:
        """

        return self

    def apply(self, e):
        """
        Transforms given element using added conditions.

        If no condition returns True on element e then returns e.

        Note that ChainedCondition pipeline must be closed
        before invoking this method.

        :param e:
        :return:
        """

        if self._closed is False:
            raise PipelineNOTClosed('close operation such as else_ '
                                    'or done has not been invoked.')

        for condition in self._conditions:
            y = condition.apply(e)

            if y is not EMPTY:
                return y.get()
        else:
            return e

    def default_name(self) -> str:
        size = len(self._conditions)

        if size == 0:
            return 'ChainedCondition has not defined any condition'

        if size == 1:
            return "ChainedCondition defines 'if' condition"

        if self._else_called:
            if size == 2:
                return "ChainedCondition defines 'if' and 'else' condition"

            return ("ChainedCondition defines 'if' then {} elif condition{} "
                    "and 'else' condition".format(size - 2, 's' if size > 3 else ''))
        else:
            return ("ChainedCondition defines 'if' then {} elif condition{}"
                    .format(size - 1, 's' if size > 2 else ''))

    def __str__(self):
        return self._name or self.default_name()

    def __repr__(self):
        return str(self)


if __name__ == 'streamAPI.stream.streamHelper':
    __all__ = get_functions_clazz(__name__, __file__)
