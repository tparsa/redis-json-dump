from abc import ABC, abstractmethod
from typing import Iterable, Tuple


class RedisIO(ABC):
    @abstractmethod
    def __iter__(self) -> Iterable[Tuple[str, str, any]]:
        ...

    @abstractmethod
    def write(self, key: str, _type: str, val: any) -> None:
        ...

    @abstractmethod
    def flush(self) -> None:
        ...


class DataDumper(ABC):
    @abstractmethod
    def dump(self, scanner: RedisIO):
        ...

    @abstractmethod
    def restore(self, scanner: RedisIO):
        ...
