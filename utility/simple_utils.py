from psycopg2 import connect
from psycopg2.extensions import connection
from psycopg2.extras import DictConnection

from utility.class_utils import constructor_setter


def dump_obj(obj, file_path, indend=1):
    with open(file_path, 'w') as f:
        from json import dump
        dump(obj, f, indent=indend)


def load_obj(file_name):
    with open(file_name) as f:
        from json import load
        return load(f)


class DB:
    @constructor_setter
    def __init__(self, *, dbname, user, password, host='localhost', port=5432):
        pass

    @property
    def dict_conn(self) -> DictConnection:
        return connect(connection_factory=DictConnection, **self.__dict__)

    @property
    def conn(self) -> connection:
        return connect(**self.__dict__)


def filter_transform(l: list, filt, transform):
    for item in l:
        if filt(item):
            yield transform(item)
