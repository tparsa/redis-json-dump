from abc import ABC, abstractmethod

import redis


class RedisTypeHandler(ABC):
    @abstractmethod
    def call_for(self, cli: redis.Redis, key: str):
        ...

    @abstractmethod
    def write(self, cli: redis.Redis, key: str, val: any):
        ...

    @abstractmethod
    def process_result(self, val):
        ...


class BasicTypeHandler(RedisTypeHandler):
    def __init__(self, getter, setter, processor) -> None:
        self.__getter = getter
        self.__setter = setter
        self.__processor = processor

    def call_for(self, cli, key):
        self.__getter(cli, key)

    def write(self, cli: redis.Redis, key: str, val: any):
        self.__setter(cli, key, val)

    def process_result(self, val):
        return self.__processor(val)


identity = lambda val: val
StringHandler = BasicTypeHandler(
    lambda cli, key: cli.get(key), lambda cli, key, val: cli.set(key, val), identity
)
SetHandler = BasicTypeHandler(
    lambda cli, key: cli.smembers(key), lambda cli, key, val: cli.sadd(key, *val), list
)
ListHandler = BasicTypeHandler(
    lambda cli, key: cli.lrange(key, 0, -1),
    lambda cli, key, val: cli.lpush(key, *val),
    identity,
)
HashHandler = BasicTypeHandler(
    lambda cli, key: cli.hgetall(key),
    lambda cli, key, val: cli.hmset(key, val),
    identity,
)
ZSetHandler = BasicTypeHandler(
    lambda cli, key: cli.zrange(key, 0, -1, withscores=True),
    lambda cli, key, val: cli.zadd(key, dict(val)),
    identity,
)