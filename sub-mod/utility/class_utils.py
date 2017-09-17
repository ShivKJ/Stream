from functools import wraps
from inspect import FullArgSpec, getfullargspec


class VarArgPresent(Exception):
    pass


def __same_name_as_constructor(ins: FullArgSpec, *args, **kwargs):
    if ins.varargs is not None:
        raise VarArgPresent('variable argument is present.')

    pos_args_names = ins.args

    obj_dict = {}

    obj_dict.update(zip(pos_args_names[1:len(args) + 1], args))

    if ins.defaults:
        key_args_names = ins.args[-len(ins.defaults):]
        obj_dict.update(dict(zip(key_args_names, ins.defaults)))
    else:
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
