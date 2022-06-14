from typing import IO
import json
import csv
import progressbar

from abstract_redis import DataDumper, RedisIO


class JSONDumper(DataDumper):
    def __init__(self, f: IO, preserve_ttls: bool = False) -> None:
        self.__file = f
        self._preserve_ttls = preserve_ttls

    def dump(self, io: RedisIO):
        total_keys = io.count_keys()
        bar = progressbar.ProgressBar(maxval=total_keys, \
            widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        number_of_dumped_keys = 0
        bar.start()
        for _type, key, val, ttl in io:
            obj = {"key": key, "type": _type, "value": val, "ttl": ttl}
            if not self._preserve_ttls:
                del obj["ttl"]
            json.dump(obj, self.__file)
            number_of_dumped_keys += 1
            bar.update(number_of_dumped_keys)
            self.__file.write("\n")
        bar.finish()

    def restore(self, io: RedisIO):
        for line in self.__file:
            j = json.loads(line)
            io.write(j["key"], j["type"], j["value"], j["ttl"])
        io.flush()


class CSVDumper(DataDumper):
    def __init__(self, f: IO) -> None:
        self.__file = f

    def dump(self, io: RedisIO):
        writer = csv.writer(self.__file)
        total_keys = io.count_keys()
        bar = progressbar.ProgressBar(maxval=total_keys, \
            widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        number_of_dumped_keys = 0
        bar.start()
        for _type, key, val in io:
            writer.writerow([key, _type, val])
            number_of_dumped_keys += 1
            bar.update(number_of_dumped_keys)
        bar.finish()

    def restore(self, io: RedisIO):
        reader = csv.reader(self.__file)
        for key, _type, val in reader:
            io.write(key, _type, val)
