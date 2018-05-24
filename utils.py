import json
from csv import DictReader
from datetime import date, datetime, timedelta
from functools import partial
from itertools import chain
from os import walk
from os.path import abspath, join

from dateutil.parser import parse


def files_inside_dir(dir_name, match=lambda x: True,
                     mapper=lambda x: x,
                     as_itr=False, sort_key=False):
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

        if sort_key is not False:
            it.sort(key=sort_key)

    return it


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
