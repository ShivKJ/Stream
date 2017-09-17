from utility.class_utils import constructor_setter


class A:
    @constructor_setter
    def __init__(self, a, *, e=1, d=2, f):
        pass


a = A(10, e='shiv', f='jaiswal')
print(a.__dict__)
