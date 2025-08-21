"""
author: Shiv
email: shivkj001@gmail.com
"""

from csv import DictReader, reader as ListReader
from datetime import date, datetime, timedelta
from functools import partial, singledispatch
from operator import itemgetter
from os import walk
from os.path import abspath, join
from typing import Callable, Dict, Iterable, List, Tuple, Union

from streamAPI.utility.Types import DateTime, Filter, Function, PathGenerator, T, X, Y

NIL = object()


def filter_transform(itr: Iterable[T],
                     condition: Filter[T],
                     transform: Function[T, X]) -> Iterable[X]:
    """
    given a iterator filters elements using "condition" and transform filtered element using "transform".

    Example:
        for e in filter_transform(range(10),lambda x:x%2==0,lambda x:x**2):
            print(e,end=' ')

        prints: 0 4 16 36 64

    :param itr: data source
    :param condition: filtering condition.
    :param transform: maps filtered elements.

    :return: iterator made from first filtering and then transforming filtered elements.
    """

    return map(transform, filter(condition, itr))


def always_true(e) -> bool:
    return True


def identity(e: T) -> T:
    return e


def _files_inside_dir(dir_name: str,
                      match: Filter[str] = always_true,
                      append_full_path=True) -> PathGenerator:
    """
    recursively finds all files inside dir and in its subdir recursively.
    Each out file name will have complete path

    :param dir_name: top level dir
    :param match: criteria to select file
    :param append_full_path: if full path is to be given as output
    :return: generator to files
    """

    if append_full_path:
        dir_name = abspath(dir_name)

    for dir_path, _, files in walk(dir_name):
        dir_joiner: Function[str, str] = partial(join, dir_path)
        yield from filter(match, map(dir_joiner, files))


def csv_itr(file: str, as_dict=True) -> Iterable[Dict[str, str]]:
    """
    returns a generator from reading csv file.
    Each row is returned as dictionary.

    :param file:
    :param as_dict:
    :return: row of csv
    """
    with open(file) as f:
        yield from DictReader(f) if as_dict else ListReader(f)


csv_ListReader: Function[str, Iterable[List[str]]] = partial(csv_itr, as_dict=False)


# -----------------------------------------------------


@singledispatch
def as_date(date_: DateTime) -> date:
    """
    create date object from given "date_". If "date_" is of type string, then
    it must to of type "YYYY-MM-DD".

    :param date_:
    :return: date object made from "date_"
    """

    raise TypeError('input is of type: {}; '
                    'which is undefined.'.format(type(date_)))


@as_date.register(str)
def _(date_):
    from dateutil.parser import parse

    return parse(date_).date()


@as_date.register(datetime)
def _(date_): return date_.date()


@as_date.register(date)
def _(date_): return date_


def date_generator(start_date: DateTime, end_date: DateTime,
                   include_end: bool = True, interval: int = 1) -> Iterable[date]:
    """
    generates dates between start and end date.

    Example:
        for date_ in date_generator('2017-01-01','2017-01-10',include_end=False,interval=3):
            print(date_,end=' ')
        prints: 2017-01-01 2017-01-04 2017-01-07

        for date_ in date_generator('2017-01-01','2017-01-10',include_end=True,interval=3):
            print(date_,end=' ')
        prints: 2017-01-01 2017-01-04 2017-01-07 2017-01-10

    :param start_date:
    :param end_date:
    :param include_end: defaults to True
    :param interval: defaults to 1
    :return: generator of date using given arguments.
    """

    start_date = as_date(start_date)
    end_date = as_date(end_date)

    if include_end:
        assert start_date <= end_date, 'start date must be less than or equal to end_date'
    else:
        assert start_date < end_date, 'start date must be less than end_date'
        end_date -= timedelta(days=1)

    td = timedelta(days=interval)

    while start_date <= end_date:
        yield start_date

        start_date += td


# -----------------------------------------------------

def divide_in_chunk(docs: Iterable[T], chunk_size: int) -> Iterable[Tuple[T, ...]]:
    """
    divides list of elements in fixed size of chunks.
    Last chunk can have elements less than chunk_size.

    :param docs: list of elements
    :param chunk_size:
    :return: generator for chunks
    """
    assert chunk_size != 0, 'chunk size can not be zero'

    itr = iter(docs)
    rng = range(chunk_size)

    chunk = get_chunk(itr, rng)

    while chunk:
        yield chunk
        chunk = get_chunk(itr, rng)


def get_chunk(itr: Iterable[T], rng: Union[range, int],
              return_type: Callable[[Iterable[X]], Y] = tuple) -> Y:
    """
    fetching one chunk from itr using rng class object.
    In case, rng is an integer, it is converted as rng = range(rng)

    Returned chunk size will be minimum of itr size and chunk size.

    Note that: rng size can not be zero. If rng is not of type range then
               it must be of type int which will represent chunk size and
               it should be positive.

    :param itr:
    :param rng: defines chunk size
    :param return_type:
    :return:
    """

    if not isinstance(rng, (range, int)):
        raise TypeError(f"rng should be either of type 'range' or "
                        "'int' but given: {type(rng)}")

    if isinstance(rng, int):
        assert rng > 0, 'chunk size must be positive'
        rng = range(rng)

    if not rng:
        raise ValueError("Specified 'rng' argument is invalid")

    return return_type(item[1] for item in zip(rng, itr))


# -------------------------- Comparator ----------------------------------
def default_comp(a: X, b: X, func: Function[X, Y] = None) -> int:
    """
    compares 'a' and 'b'. (__lt__, __eq__ methods must be implemented by
    corresponding 'class')

    1) If a is equal to b then returns 0
    2) If a is less than b then returns -1
    3) else return 1

    :param a:
    :param b:
    :param func: if not None, then comparison is made on func(a) and func(b)

    :return:
    """

    if func is not None:
        a = func(a)
        b = func(b)

    if a < b:
        return -1

    if a == b:
        return 0

    return 1


def comparing(func: Function):
    """
    returns "default_comp" but sets "func" kwarg using given input.

    :param func:
    :return:
    """

    return partial(default_comp, func=func)


# ------------ importing function defined only in this module-------------


def get_functions_clazz(module_name: str, script_path: str) -> tuple:
    """
    returns collection of function and class in a module not starting with '_'
    :param module_name: __name__
    :param script_path:__file__
    :return:
    """
    from inspect import getmembers, getmodule, isfunction, isclass
    from importlib import import_module

    module = import_module(module_name, script_path)

    def predicate(o: tuple) -> bool:
        n, m = o
        return (getmodule(m) is module and not n.startswith('_')
                and (isclass(m) or isfunction(m)))

    return tuple(filter_transform(getmembers(module), predicate, itemgetter(0)))


if __name__ == 'streamAPI.utility.utils':
    __all__ = get_functions_clazz(__name__, __file__) + ('csv_ListReader',)
