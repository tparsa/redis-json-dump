from typing import ByteString, Iterable, Tuple, List
from base64 import b64decode, b64encode
import redis
import rediscluster

from abstract_redis import RedisIO
from .type_handlers import (
    HashHandler,
    ListHandler,
    NoneHandler,
    SetHandler,
    StringHandler,
    ZSetHandler,
)
from .utils import to_batch


is_number = lambda x: isinstance(x, int) or isinstance(x, float)
is_iterable = lambda x: isinstance(x, Iterable) and not isinstance(x, ByteString) and not isinstance(x, str)


def b64applier(x, func):
    if isinstance(x, dict):
        return {func(k):func(v) for k, v in x.items()}
    if is_iterable(x):
        return [b64applier(i, func) for i in x]
    return func(x)


def b64enc(x):
    if x is None:
        return b64applier(b"", b64encode)
    return b64applier(x, b64encode)


def b64dec(x):
    if x is None:
        return b64applier("", b64decode)
    return b64applier(x, b64decode)


def decode(x):
    if isinstance(x, dict):
        return {k.decode('utf-8'):v.decode('utf-8') for k, v in x.items()}
    if is_iterable(x):
        return [decode(i) for i in x]
    return x.decode('utf-8')


class RedisPatternIO(RedisIO):
    def __init__(self, cli: redis.Redis, pattern: str = None):
        if pattern is None:
            pattern = "*"

        self.pattern = pattern
        self.cli = cli
        self.pipe = self.cli.pipeline()
        self.type_handlers = {
            "string": StringHandler,
            "set": SetHandler,
            "list": ListHandler,
            "hash": HashHandler,
            "zset": ZSetHandler,
            "none": NoneHandler,
        }
    
    def count_keys(self) -> int:
        keys = self.cli.scan_iter("*", count=10000)
        return sum(1 for i in keys)

    def get_types(self, keys: List[str]) -> List[str]:
        for k in keys:
            self.pipe.type(k)
        return list(map(decode, self.pipe.execute()))

    def get_ttls(self, keys: List[str]) -> List[str]:
        for k in keys:
            self.pipe.ttl(k)
        return self.pipe.execute()

    def get_values(self, types: List[str], keys: List[str]) -> List[any]:
        for _type, key in zip(types, keys):
            self.type_handlers[_type].call_for(self.pipe, key)
        result = list(map(decode, map(b64enc, self.pipe.execute())))
        return [self.type_handlers[t].process_result(r) for t, r in zip(types, result)]

    def __iter__(self) -> Iterable[Tuple[str, str, any, int]]:
        """
        Iterable[(type, key, value, ttl)]
        """
        keys = self.cli.scan_iter("*", count=10000)
        for batch in to_batch(keys, 10000):
            types = self.get_types(batch)
            ttls = self.get_ttls(batch)
            yield from zip(types, decode(batch), self.get_values(types, batch), ttls)

    def write(self, key: str, _type: str, val: any, ttl: int) -> None:
        self.type_handlers[_type].write(self.pipe, key, b64dec(val), ttl)
        if len(self.pipe.command_stack) > 1000:
            self.pipe.execute()

    def flush(self) -> None:
        if len(self.pipe.command_stack) > 0:
            self.pipe.execute()


class RedisSingleIO(RedisPatternIO):
    def __init__(self, uri: str, pattern: str = None):
        cli = redis.Redis.from_url(uri, decode_responses=False)
        super().__init__(cli, pattern)


class RedisClusterIO(RedisPatternIO):
    def __init__(self, uri: str, pattern: str = None):
        class CustomConnection(redis.Connection):
            def __init__(self, *args, **kwargs):
                kwargs["decode_responses"] = False
                super().__init__(*args, **kwargs)

        cli = rediscluster.RedisCluster.from_url(uri, connection_class=CustomConnection)
        super().__init__(cli, pattern)
