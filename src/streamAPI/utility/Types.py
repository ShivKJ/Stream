"""
author: Shiv
email: shivkj001@gmail.com
"""

from datetime import date, datetime
from typing import Callable, Iterable, TypeVar, Union

T = TypeVar('T')
X = TypeVar('X')
Y = TypeVar('Y')
Z = TypeVar('Z')

Function = Callable[[X], Y]
BiFunction = Callable[[X, Y], Z]
Consumer = Callable[[X], None]
Filter = Callable[[X], bool]
PathGenerator = Iterable[str]
DateTime = Union[str, date, datetime]
