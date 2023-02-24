import sys
import random
import subprocess

from redis import Redis

from ..redis_lib.type_handlers import (
    StringHandler,
    HashHandler,
    SetHandler,
    ListHandler,
    ZSetHandler
)
from ..mock.args import ArgsMock


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


class Test:
    def __init__(self, count_foreach, source_redis_url, dest_redis_url):
        self.setUpClass(count_foreach, source_redis_url, dest_redis_url)
    
    def setUpClass(self, count_foreach, source_redis_url, dest_redis_url):
        self.count_foreach = count_foreach
        self.source_redis = Redis.from_url(source_redis_url)
        self.source_redis.flushall()
        self.dest_redis = Redis.from_url(dest_redis_url)
        self.dest_redis.flushall()

        for type in TEST_TYPES:
            for i in range(count_foreach):
                HANDLERS[type].write(self.source_redis, f"{KEY_PREFIX}:{type}:{i}", VALUE_GENERATOR[type](), -1)

        restore_file = "dump_single_redis_integration_test.json"

        self.dump_args = ArgsMock()
        self.dump_args.add("uri", source_redis_url)
        self.dump_args.add("output_stdout", False)
        self.dump_args.add("file", restore_file)
        self.dump_args.add("mode", "dump")
        self.dump_args.add("type", "single")
        self.dump_args.add("db", 0)
        self.dump_args.add("ttl", True)

        self.restore_args = ArgsMock()
        self.restore_args.add("uri", dest_redis_url)
        self.restore_args.add("input_stdin", False)
        self.restore_args.add("file", restore_file)
        self.restore_args.add("mode", "restore")
        self.restore_args.add("type", "single")
        self.restore_args.add("db", 0)
        self.restore_args.add("ttl", True)


    def test(self):
        from ..main import CLI
        CLI(self.dump_args).execute()
        CLI(self.restore_args).execute()

        self._check_integrity()
    
    def _check_integrity(self):
        self._check_integrity_of_two_redises(self.source_redis, self.dest_redis)
        self._check_integrity_of_two_redises(self.dest_redis, self.source_redis)

    def _check_integrity_of_two_redises(self, redis1, redis2):
        for key in self.source_redis.keys():
            self._check_key_existence(key, redis1, redis2)
            self._check_single_key_value_integrity(key, redis1, redis2)
            self._check_single_key_ttl_integrity(key, redis1, redis2)

    def _check_key_existence(self, key, redis1, redis2):
        assert redis1.exists(key) == redis2.exists(key)
    
    def _check_single_key_value_integrity(self, key, redis1, redis2):
        data_type = redis1.type(key)
        if data_type:
            data_type = data_type.decode("utf-8")
            val1 = HANDLERS[data_type].call_for(redis1, key)
            val2 = HANDLERS[data_type].call_for(redis2, key)
            DIFF_CHECKER[data_type](val1, val2)
    
    def _check_single_key_ttl_integrity(self, key, redis1, redis2):
        ttl1 = redis1.ttl(key)
        ttl2 = redis2.ttl(key)

        assert abs(ttl1 - ttl2) < 5
        