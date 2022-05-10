from typing import Iterable, Tuple, List
import redis
import rediscluster

from abstract_redis import RedisIO
from .type_handlers import (
    HashHandler,
    ListHandler,
    SetHandler,
    StringHandler,
    ZSetHandler,
)
from .utils import to_batch


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
        }

    def get_types(self, keys: List[str]) -> List[str]:
        for k in keys:
            self.pipe.type(k)
        return self.pipe.execute()

    def get_ttls(self, keys: List[str]) -> List[str]:
        for k in keys:
            self.pipe.ttl(k)
        return self.pipe.execute()

    def get_values(self, types: List[str], keys: List[str]) -> List[any]:
        for _type, key in zip(types, keys):
            self.type_handlers[_type].call_for(self.pipe, key)
        result = self.pipe.execute()
        return [self.type_handlers[t].process_result(r) for t, r in zip(types, result)]

    def __iter__(self) -> Iterable[Tuple[str, str, any, int]]:
        """
        Iterable[(type, key, value)]
        """
        keys = self.cli.scan_iter("*", count=10000)
        for batch in to_batch(keys, 10000):
            types = self.get_types(batch)
            ttls = self.get_ttls(batch)
            yield from zip(types, batch, self.get_values(types, batch), ttls)

    def write(self, key: str, _type: str, val: any, ttl: int) -> None:
        self.type_handlers[_type].write(self.pipe, key, val, ttl)
        if len(self.pipe.command_stack) > 1000:
            self.pipe.execute()

    def flush(self) -> None:
        if len(self.pipe.command_stack) > 0:
            self.pipe.execute()


class RedisSingleIO(RedisPatternIO):
    def __init__(self, uri: str, pattern: str = None):
        cli = redis.Redis.from_url(uri, decode_responses=True)
        super().__init__(cli, pattern)


class RedisClusterIO(RedisPatternIO):
    def __init__(self, uri: str, pattern: str = None):
        class CustomConnection(redis.Connection):
            def __init__(self, *args, **kwargs):
                kwargs["decode_responses"] = True
                super().__init__(*args, **kwargs)

        cli = rediscluster.RedisCluster.from_url(uri, connection_class=CustomConnection)
        super().__init__(cli, pattern)
