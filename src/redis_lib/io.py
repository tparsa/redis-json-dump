from typing import ByteString, Iterable, Tuple, List
from base64 import b64decode, b64encode
import redis

from src.abstract_redis import RedisIO
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
        return {b64applier(k, func):b64applier(v, func) for k, v in x.items()}
    if is_iterable(x):
        return [b64applier(i, func) for i in x]
    if isinstance(x, bytes):
        return func(x)
    else:
        return func(str(x).encode("utf-8"))


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
        return {decode(k):decode(v) for k, v in x.items()}
    if is_iterable(x):
        return [decode(i) for i in x]
    if isinstance(x, bytes):
        return x.decode('utf-8')
    else:
        return x


class RedisPatternIO(RedisIO):
    def __init__(self, cli: redis.Redis, pattern: str = None):
        if pattern is None:
            pattern = "*"

        self.pattern = pattern
        self.cli = cli
        self.pipe = self.cli.pipeline(transaction=False)
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
        x = self.pipe.execute()
        return list(map(decode, x))

    def get_ttls(self, keys: List[str]) -> List[str]:
        for k in keys:
            self.pipe.ttl(k)
        return self.pipe.execute()

    def get_values(self, types: List[str], keys: List[str]) -> List[any]:
        for _type, key in zip(types, keys):
            self.type_handlers[_type].call_for(self.pipe, key)
        result = list(map(decode, map(b64enc, map(decode, self.pipe.execute()))))
        return [self.type_handlers[t].process_result(r) for t, r in zip(types, result)]

    def __iter__(self) -> Iterable[Tuple[str, str, any, int]]:
        """
        Iterable[(type, key, value, ttl)]
        """
        keys = self.cli.scan_iter(self.pattern, count=10000)
        for batch in to_batch(keys, 10000):
            b = decode(batch)
            types = self.get_types(b)
            ttls = self.get_ttls(b)
            yield from zip(types, b, self.get_values(types, b), ttls)

    def write(self, key: str, _type: str, val: any, ttl: int) -> None:
        self.cli.delete(key)
        self.type_handlers[_type].write(self.pipe, key, decode(b64dec(val)), ttl)
        if len(self.pipe.command_stack) > 1000:
            self.pipe.execute()

    def flush(self) -> None:
        if len(self.pipe.command_stack) > 0:
            self.pipe.execute()


class RedisSingleIO(RedisPatternIO):
    def __init__(self, uri: str = None, pattern: str = None, cli: redis.Redis = None):
        _cli = cli
        if _cli is None:
            _cli = redis.Redis.from_url(uri, decode_responses=False)
        super().__init__(_cli, pattern)


class RedisClusterIO(RedisPatternIO):
    def __init__(self, uri: str = None, pattern: str = None, cli: redis.cluster.RedisCluster = None):
        class CustomConnection(redis.Connection):
            def __init__(self, *args, **kwargs):
                kwargs["decode_responses"] = False
                super().__init__(*args, **kwargs)
        if cli is None:
            initiator_cli = redis.cluster.RedisCluster.from_url(uri, connection_class=CustomConnection)
        else:
            initiator_cli = cli
        clis = [node.redis_connection for node in initiator_cli.get_nodes()]
        self._ios = [RedisPatternIO(cli, pattern) for cli in clis]
        self.cli = self._ios[0].cli
        self.pipe = self._ios[0].pipe
        self._write_io = RedisPatternIO(initiator_cli)

    def count_keys(self) -> int:
        ret = 0
        for io in self._ios:
            ret += io.count_keys()
        return ret

    def get_types(self, keys: List[str]) -> List[str]:
        return self._ios[0].get_types(keys)

    def get_ttls(self, keys: List[str]) -> List[str]:
        return self._ios[0].get_ttls(keys)

    def get_values(self, types: List[str], keys: List[str]) -> List[any]:
        return self._ios[0].get_values(types, keys)

    def __iter__(self) -> Iterable[Tuple[str, str, any, int]]:
        for io in self._ios:
            for item in io:
                yield item

    def write(self, key: str, _type: str, val: any, ttl: int) -> None:
        self._write_io.write(key, _type, val, ttl)

    def flush(self) -> None:
        for io in self._ios:
            io.flush()
