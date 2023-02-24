import random

from ..redis_lib.type_handlers import (
    StringHandler,
    HashHandler,
    SetHandler,
    ListHandler,
    ZSetHandler
)

KEY_PREFIX = "test"
TEST_TYPES = [
    "string",
    "hash",
    "set",
    "list",
    "zset"
]

HANDLERS = {
    "string": StringHandler,
    "hash": HashHandler,
    "set": SetHandler,
    "list": ListHandler,
    "zset": ZSetHandler
}


def value_generator_string():
    import string
    return ''.join(random.choices(string.ascii_lowercase, k=10))


def value_generator_hash():
    number_of_keys = int(random.random() * 10) + 3
    ret = {}

    for i in range(number_of_keys):
        key = "key:" + str(i)
        value = value_generator_string()

        ret[key] = value
    
    return ret


def value_generator_set():
    number_of_members = int(random.random() * 10) + 3
    ret = set()

    for i in range(number_of_members):
        member = int(random.random() * 100)
        while member in ret:
            member = int(random.random() * 100)
        ret.add(member)
    
    return ret


def value_generator_list(size=0):
    if not size:
        number_of_members = int(random.random() * 10) + 3
    else:
        number_of_members = size
    ret = list()

    for i in range(number_of_members):
        member = int(random.random() * 100)
        ret.append(member)
    
    return ret


def value_generator_zset():
    number_of_members = int(random.random() * 10) + 3
    values = value_generator_list(number_of_members)
    scores = value_generator_list(number_of_members)
    return {a[0]:a[1] for a in zip(values, scores)}


VALUE_GENERATOR = {
    "string": value_generator_string,
    "hash": value_generator_hash,
    "set": value_generator_set,
    "list": value_generator_list,
    "zset": value_generator_zset
}


def diff_checker_string(val1, val2):
    assert val1 == val2


def diff_checker_hash(val1, val2):
    for key in val1.keys():
        assert key in val2
        assert val1[key] == val2[key]

    for key in val2.keys():
        assert key in val1


def diff_checker_set(val1, val2):
    assert len(val1) == len(val2)
    for i in val1:
        assert i in val2


def diff_checker_list(val1, val2):
    assert len(val1) == len(val2)
    for i in val1:
        assert i in val2


def diff_checker_zset(val1, val2):
    f = lambda x: {a[0]: a[1] for a in x}
    val1_dict = f(val1)
    val2_dict = f(val2)
    diff_checker_hash(val1_dict, val2_dict)


DIFF_CHECKER = {
    "string": diff_checker_string,
    "hash": diff_checker_hash,
    "set": diff_checker_set,
    "list": diff_checker_list,
    "zset": diff_checker_zset
}
