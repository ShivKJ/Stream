from functools import wraps

from utility.class_utils import update_with_values_and_constructor_arg_name


def p(f):
    @wraps(f)
    def get(cls, *args, **kwargs):
        obj = object.__new__(cls)
        update_with_values_and_constructor_arg_name(obj, *args, **kwargs)
        return obj

    return get


class A:
    def __init__(self, a, *, e=1, d=2, f):
        print('init')
        pass

    @p
    def __new__(cls, *args, **kwargs):
        pass


a = A(10, e='shiv', f='jaiswal')
print(a.__dict__)
