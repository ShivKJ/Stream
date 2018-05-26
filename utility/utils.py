import json
from csv import DictReader
from datetime import date, datetime, timedelta
from functools import partial, wraps
from inspect import FullArgSpec, getfullargspec
from itertools import chain
from logging import getLogger
from os import walk
from os.path import abspath, join
from time import time

from dateutil.parser import parse
from psycopg2 import connect
from psycopg2.extensions import connection
from psycopg2.extras import DictConnection

from utility.logger import LOGGER_NAME

logger = getLogger(LOGGER_NAME)


# -----------------------------------------------------


class DB:
    def __init__(self, *, dbname, user, password, host='localhost', port=5432):
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


# -----------------------------------------------------
class VarArgPresent(Exception):
    pass


def __same_name_as_constructor(ins: FullArgSpec, *args, **kwargs):
    if ins.varargs is not None:
        raise VarArgPresent('variable argument is present.')

    pos_args_names = ins.args

    obj_dict = {}

    obj_dict.update(zip(pos_args_names[1:len(args) + 1], args))

    if ins.defaults is not None:
        key_args_names = ins.args[-len(ins.defaults):]
        obj_dict.update(dict(zip(key_args_names, ins.defaults)))

    if ins.kwonlydefaults is not None:
        # when key_only args are also used (* is used)
        obj_dict.update(ins.kwonlydefaults)

    obj_dict.update(**kwargs)  # now overriding with given kwargs
    return obj_dict


def constructor_setter(__init__):
    @wraps(__init__)
    def f(self, *args, **kwargs):
        ins = getfullargspec(__init__)
        self.__dict__.update(__same_name_as_constructor(ins, *args, **kwargs))
        __init__(self, *args, **kwargs)

    return f


def execution_time(func):
    '''
    finds time taken to execute a function.
    Function should not be recursive
    :param func:
    :return:
    '''

    @wraps(func)
    def f(*args, **kwargs):
        start_time = time()
        output = func(*args, **kwargs)
        logger.info('time taken to execute %s: %0.3f seconds',
                    func.__name__, time() - start_time)
        return output

    return f


# -----------------------------------------------------

def files_inside_dir(dir_name, match=lambda x: True,
                     mapper=lambda x: x,
                     as_itr=False):
    """
    recursively finds all files inside dir and in its subdir recursively
    :param dir_name: top level dir
    :param match: criteria to select file
    :param mapper: transforming selected files
    :param as_itr: if output is required to be iterator
    :return: file path generator / list
    """
    maps = []

    dir_name = abspath(dir_name)
    for dir_path, _, files in walk(dir_name):
        dir_joiner = partial(join, dir_path)
        maps.append(filter(match, map(dir_joiner, files)))

    it = map(mapper, chain.from_iterable(maps))

    if not as_itr:
        it = list(it)

    return it


def get_file_name(file_name: str, at=-1) -> str:
    return file_name.split('/')[at].split('.')[0]


def json_load(file: str):
    with open(file) as f:
        return json.load(f)


def json_dump(obj, file: str, indent: int = None, default_cast=None,
              sort_keys=False, cls=None):
    with open(file, 'w') as f:
        json.dump(obj, f, indent=indent,
                  default=default_cast, sort_keys=sort_keys,
                  cls=cls)


def csv_itr(file: str) -> dict:
    with open(file) as f:
        reader = DictReader(f)
        for doc in reader:
            yield doc


# -----------------------------------------------------
def as_date(date_) -> date:
    '''
    cast date_ to date object.

    :param date_:
    :return: date object from "date_"
    '''
    if isinstance(date_, str):
        date_ = parse(date_)

    if isinstance(date_, datetime):
        date_ = date_.date()

    assert isinstance(date_, date)

    return date_


def date_generator(start_date, end_date, include_end=True, days=1):
    '''
    generates dates between start and end date (both inclusive)
    :param start_date:
    :param end_date:
    :param days:
    :return:
    '''

    start_date = as_date(start_date)
    end_date = as_date(end_date)

    if include_end:
        assert start_date <= end_date
    else:
        assert start_date < end_date
        end_date -= timedelta(days=1)

    td = timedelta(days=days)

    while start_date <= end_date:
        yield start_date

        start_date += td


# -----------------------------------------------------

def divide_in_chunk(docs: list, chunk_size):
    '''
    divides list of elements in fixed size of chunks.
    Last chunk can have elements less than chunk_size.

    :param docs: list of elements
    :param chunk_size:
    :return: iterator
    '''
    if len(docs) <= chunk_size:
        yield docs
    else:
        for i in range(0, len(docs), chunk_size):
            yield docs[i:i + chunk_size]


def filter_transform(l: list, condition, transform):
    for item in l:
        if condition(item):
            yield transform(item)


# ------------ importing function defined only in this module-------------


def get_functions_clazz(module=__name__) -> tuple:
    from importlib import import_module
    from inspect import getmembers, getmodule, isfunction, isclass
    from operator import itemgetter

    module = import_module(module)

    def predicate(o: tuple) -> bool:
        n, m = o
        return getmodule(m) is module and not n.startswith('_') and (isclass(m) or isfunction(m))

    return tuple(map(itemgetter(0), filter(predicate, getmembers(module))))


if __name__ == 'utility.utils':
    __all__ = get_functions_clazz('utility.utils')
