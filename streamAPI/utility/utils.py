import json
from csv import DictReader, reader as ListReader
from datetime import date, datetime, timedelta
from functools import partial, wraps
from inspect import FullArgSpec, getfullargspec
from logging import getLogger
from operator import itemgetter
from os import walk
from os.path import abspath, join
from typing import Callable, Dict, Iterable, List, Tuple, TypeVar, Union

from dateutil.parser import parse
from psycopg2 import connect
from psycopg2.extensions import connection
from psycopg2.extras import DictConnection
from time import time

T = TypeVar('T')
Z = TypeVar('Z')


# --------------------------- The decorators -----------------------------------
def execution_time(logger_name: str = None, prefix: str = None):
    """
    logs time taken to execute a function to file associated with logger_name.
    if logger_name is None then creates a log file in current dir to log execution time,

    example:

        @execution_time()
        def function(*args,**kwargs):
            pass

    :param logger_name:
    :param prefix: to be added before every logging message
    :return a decorator which will applied on function
    """

    if logger_name is None:
        from streamAPI.utility.logger import LOGGER_NAME
        logger_name = LOGGER_NAME

    def message(m: str) -> str:
        """
        adds prefix before the message m if prefix is not None
        :param m:
        :return:
        """
        if prefix is not None:
            m = prefix + ' : ' + m

        return m

    def _execution_time(func):
        """
        A decorator to log execution time of function.

        :param func:
        :return: wrapping function
        """
        logger = getLogger(logger_name)

        @wraps(func)
        def f(*args, **kwargs):
            start_time = time()
            output = func(*args, **kwargs)

            logger.info(message('time taken to execute "%s": %0.3f seconds'),
                        func.__name__, time() - start_time)
            return output

        return f

    return _execution_time


class VarArgPresent(Exception):
    pass


def constructor_setter(throw_var_args_exception=True):
    """
    This decorator sets objects attribute name same as defined in its constructor.
    kwargs keys also contribute to object attribute along with key only args.

    In case, variable argument is present in constructor, Exception VarArgPresent is thrown.

    Example 1:

    class Foo:
        @constructor_setter(throw_var_args_exception=True)
        def __init__(self, a, b, **kwargs):
            pass

    kwargs = dict(p=3, q=4)

    foo = Foo(10,20, **kwargs)
    print(foo.a, foo.b, foo.p, foo.q)

    Example 2:

    class Foo:
        @constructor_setter(throw_var_args_exception=True)
        def __init__(self, a, *, e, **kwargs):
            pass

    foo = Foo(1, e=2)

    print(foo.a, foo.e)

    :param throw_var_args_exception: if exception has to be thrown in case of variable args
    :return:
    """

    def _same_name_as_constructor(ins: FullArgSpec, *args, **kwargs) -> dict:
        """
        Finds attributes to be set in object
        :param ins:
        :param args:
        :param kwargs:
        :return: dictionary of keys as attr name and values as attr value.
        """
        if throw_var_args_exception and ins.varargs is not None:
            raise VarArgPresent('variable argument is present.')

        obj_dict = {}

        if ins.defaults is not None:
            '''
            def foo(a, b, p=1, q=2, *, r):pass
            
            ins.defaults is tuple (1,2)
            To get names, we use ins.args that is (a,b,p,q) here.
            
            Note that, values of p and q will be updated from kwargs if present in it.  
            '''

            key_args_names = ins.args[-len(ins.defaults):]  # picking names of default keys
            obj_dict.update(zip(key_args_names, ins.defaults))

        if ins.kwonlydefaults is not None:
            # when key_only args which comes after '*' in function definition.
            # It is not None which means they have default values
            '''
            def foo(a, b, p=1, *, q=2, r):pass
            
            ins.kwonlydefaults is dictionary dict(q=2).
            
            '''
            obj_dict.update(ins.kwonlydefaults)

        obj_dict.update(**kwargs)
        '''
        Now checking if kwonlyargs have been initialized.
        
        def foo(a, b, p=1, *, q=2, r):pass
        here kwonlyargs is [q, r].
        Note that we have already populated 'q' in dictionary obj_dict using ins.kwonlydefaults.
        '''

        for k in ins.kwonlyargs:
            if k not in obj_dict:
                raise ValueError('{} is absent in argument and it is keyonly args')

        # Updating varargs in obj_dict. '1' stands for self, ignoring it.
        obj_dict.update(zip(ins.args[1:], args))

        return obj_dict

    def _constructor_setter(__init__):
        @wraps(__init__)
        def f(self, *args, **kwargs):
            ins = getfullargspec(__init__)
            self.__dict__.update(_same_name_as_constructor(ins, *args, **kwargs))
            __init__(self, *args, **kwargs)

        return f

    return _constructor_setter


# -----------------------------------------------------


class DB:
    """
    This class provide functionality to create connection object.
    This is helpful in case mulitple object is to made for same credential

    The underline database used is Postgresql
    """

    def __init__(self, *, dbname: str,
                 user: str, password: str,
                 host: str = 'localhost', port: int = 5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    @property
    def dict_conn(self) -> connection:
        return connect(**self.__dict__, connection_factory=DictConnection)

    @property
    def conn(self) -> connection:
        return connect(**self.__dict__)

    @property
    def url(self) -> Tuple[str, str]:
        return str(self), self.password

    def __str__(self) -> str:
        return 'psql -U {user} -d {dbname} -h {host} -p {port}'.format(**self.__dict__)

    def __repr__(self) -> str:
        return str(self)


# -----------------------------------------------------
Filter = Callable[[T], bool]


def filter_transform(data_stream: Iterable[T],
                     condition: Filter,
                     transform: Callable[[T], Z]) -> Iterable[Z]:
    """
    given a list filters elements and transform filtered element
    :param data_stream:
    :param condition:
    :param transform:
    :return:
    """

    return map(transform, filter(condition, data_stream))


def _always_true(f) -> bool:
    return True


def identity(f: T) -> T:
    return f


PathGenerator = Iterable[str]


def _files_inside_dir(dir_name: str,
                      match: Filter = _always_true,
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
        dir_joiner = partial(join, dir_path)
        yield from filter(match, map(dir_joiner, files))


def files_inside_dir(dir_name: str,
                     match: Filter = _always_true,
                     as_type: Callable[[PathGenerator], T] = list,
                     append_full_path=True) -> T:
    """
    recursively finds all files inside dir and in its subdir recursively
    :param dir_name: top level dir
    :param match: criteria to select file
    :param as_type: if None then returns files as Iterator.
    :param append_full_path: if full path is to be given as output
    :return: file path generator / sequence
    """
    it = _files_inside_dir(dir_name, match=match,
                           append_full_path=append_full_path)

    return it if as_type is None else as_type(it)


def get_file_name(file_name: str, at: int = -1, split: str = '/') -> str:
    """
    Extracts fileName from a file
    :param file_name:
    :param at: fetch name after splitting files on "split"
    :param split:
    :return:
    """
    return file_name.split(split)[at].split('.')[0]


def json_load(file: str):
    """
    loads json file.
    :param file:
    :return: loaded json file as dict/list
    """

    with open(file) as f:
        return json.load(f)


def json_dump(obj, file: str, indent: int = None, default_cast=None,
              sort_keys=False, cls=None):
    """
    dumps obj in json file.
    :param obj:
    :param file:
    :param indent:
    :param default_cast:
    :param sort_keys:
    :param cls:
    """
    with open(file, 'w') as f:
        json.dump(obj, f, indent=indent,
                  default=default_cast, sort_keys=sort_keys,
                  cls=cls)


def csv_itr(file: str, as_dict=True) -> Iterable[Dict[str, str]]:
    """
    returns a generator from reading csv file.
    Each row is returned as dictionary.

    :param file:
    :param as_dict:
    :return: row of csv
    """
    with open(file) as f:
        yield from (DictReader(f) if as_dict else ListReader(f))


csv_ListReader: Callable[[str], Iterable[List[str]]] = partial(csv_itr, as_dict=False)

# -----------------------------------------------------
DateTime = Union[str, date, datetime]


def as_date(date_: DateTime) -> date:
    """
    cast date_ to date object.
    date string must be in format : YYYY-MM-DD

    :param date_:
    :return: date object from "date_"
    """
    if isinstance(date_, str):
        date_ = parse(date_)

    if isinstance(date_, datetime):
        date_ = date_.date()

    assert isinstance(date_, date)

    return date_


def date_generator(start_date: DateTime, end_date: DateTime,
                   include_end: bool = True, interval: int = 1) -> Iterable[date]:
    """
    generates dates between start and end date
    :param start_date:
    :param end_date:
    :param include_end:
    :param interval:
    :return:
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
Chunk = Tuple[T, ...]


def divide_in_chunk(docs: Iterable[T], chunk_size: int) -> Iterable[Chunk]:
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

    chunk = _next_chunk(itr, rng)

    while chunk:
        yield chunk
        chunk = _next_chunk(itr, rng)


def _next_chunk(itr: Iterable[T], rng: range) -> Chunk:
    """
    fetching one chunk from itr using rng class object
    :param itr:
    :param rng: defines chunk size
    :return:
    """
    return tuple(item[1] for item in zip(rng, itr))


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
