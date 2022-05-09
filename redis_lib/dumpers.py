from typing import IO
import json
import csv

from abstract_redis import DataDumper, RedisIO


class JSONDumper(DataDumper):
    def __init__(self, f: IO) -> None:
        self.__file = f

    def dump(self, io: RedisIO):
        for _type, key, val in io:
            json.dump({"key": key, "type": _type, "value": val}, self.__file)
            self.__file.write("\n")

    def restore(self, io: RedisIO):
        for line in self.__file:
            j = json.loads(line)
            io.write(j["key"], j["type"], j["value"])
        io.flush()


class CSVDumper(DataDumper):
    def __init__(self, f: IO) -> None:
        self.__file = f

    def dump(self, io: RedisIO):
        writer = csv.writer(self.__file)
        for _type, key, val in io:
            writer.writerow([key, _type, val])

    def restore(self, io: RedisIO):
        reader = csv.reader(self.__file)
        for key, _type, val in reader:
            io.write(key, _type, val)
