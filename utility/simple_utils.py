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




def filter_transform(l: list, filt, transform):
    for item in l:
        if filt(item):
            yield transform(item)
