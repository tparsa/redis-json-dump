from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Iterable

import redis


class RedisTypeHandler(ABC):
    @abstractmethod
    def call_for(self, cli: redis.Redis, key: str):
        raise NotImplementedError()

    @abstractmethod
    def write(self, cli: redis.Redis, key: str, val: any):
        raise NotImplementedError()

    @abstractmethod
    def process_result(self, val):
        raise NotImplementedError

class BasicTypeHandler(RedisTypeHandler):
    def __init__(self, getter, setter, processor) -> None:
        self.__getter = getter
        self.__setter = setter
        self.__processor = processor

    def call_for(self, cli, key):
        return self.__getter(cli, key)

    def write(self, cli: redis.Redis, key: str, val: any, ttl: int):
        self.__setter(cli, key, val, ttl)

    def process_result(self, val):
        return self.__processor(val)


identity = lambda val: val
StringHandler = BasicTypeHandler(
    lambda cli, key: cli.get(key),
    lambda cli, key, val, ttl: cli.setex(key, timedelta(seconds=ttl), val) if ttl >= 0 else cli.set(key, val),
    identity
)


list_if_not_none = lambda val: list(val) if val else None
SetHandler = BasicTypeHandler(
    lambda cli, key: cli.smembers(key),
    lambda cli, key, val, _: cli.sadd(key, *val) if isinstance(val, Iterable) and not isinstance(val, str) else cli.sadd(key, val),
    list_if_not_none
)


ListHandler = BasicTypeHandler(
    lambda cli, key: cli.lrange(key, 0, -1),
    lambda cli, key, val, _: cli.rpush(key, *val) if isinstance(val, Iterable) and not isinstance(val, str) else cli.rpush(key, val),
    identity,
)


HashHandler = BasicTypeHandler(
    lambda cli, key: cli.hgetall(key),
    lambda cli, key, val, _: cli.hmset(key, val),
    identity,
)


ZSetHandler = BasicTypeHandler(
    lambda cli, key: cli.zrange(key, 0, -1, withscores=True),
    lambda cli, key, val, _: cli.zadd(key, dict(val)),
    identity,
)


NoneHandler = BasicTypeHandler(
    lambda cli, key: '',
    lambda cli, key, val, _: None,
    identity,
)