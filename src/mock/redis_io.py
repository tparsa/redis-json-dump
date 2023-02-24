from typing import Iterable, Tuple
import json

from src.abstract_redis import RedisIO


class RedisIOMock(RedisIO):
    def __init__(self, data):
        self.__data = data

    def __iter__(self) -> Iterable[Tuple[str, str, any, int]]:
        return (d for d in self.__data)

    def write(self, key: str, _type: str, val: any, ttl: int) -> None:
        if (_type in ("list", "set", "zset")) and isinstance(val, str):
            val = json.loads(val.replace("'", '"'))
        if _type == "hash" and isinstance(val, str):
            val = json.loads(val.replace("'", '"'))
        self.__data.append((_type, key, val, int(ttl)))

    def flush(self) -> None:
        ...
    
    def count_keys(self) -> int:
        return len(self.__data)
    
    def get_data(self, ttl: bool = True):
        if ttl:
            return list(self.__data)
        else:
            return [(data[0], data[1], data[2], -1) for data in self.__data]