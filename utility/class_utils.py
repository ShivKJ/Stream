from inspect import getfullargspec


def update_with_values_and_constructor_arg_name(obj: object, *args, **kwargs):
    ins = getfullargspec(obj.__init__)
    pos_args_names = ins.args

    for k, v in zip(pos_args_names[1:len(args) + 1], args):
        setattr(obj, k, v)

    obj_dict = obj.__dict__

    if ins.defaults:
        # when key_only args are not present.(* is not used)
        key_args_names = ins.args[-len(ins.defaults):]
        obj_dict.update(dict(zip(key_args_names, ins.defaults)))
        obj_dict.update(**kwargs)  # now overriding with given kwargs
    else:
        obj_dict.update(ins.kwonlydefaults)
        obj_dict.update(**kwargs)
