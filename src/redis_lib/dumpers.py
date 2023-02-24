import sys
from typing import IO
import json
import csv
import progressbar

from src.abstract_redis import DataDumper, RedisIO


class JSONDumper(DataDumper):
    def __init__(
        self,
        f: IO,
        preserve_ttls: bool = False,
        enable_progress_bar: bool = True,
        log: bool = True
    ) -> None:
        self.__file = f
        self._preserve_ttls = preserve_ttls
        self._enable_progress_bar = enable_progress_bar
        self._log = log

    def dump(self, io: RedisIO):
        if self._enable_progress_bar:
            total_keys = io.count_keys()
            bar = progressbar.ProgressBar(maxval=total_keys, \
                widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
            bar.start()
        number_of_dumped_keys = 0
        for _type, key, val, ttl in io:
            obj = {"key": key, "type": _type, "value": val, "ttl": ttl}
            if not self._preserve_ttls:
                obj["ttl"] = -1
            json.dump(obj, self.__file)
            number_of_dumped_keys += 1
            if self._enable_progress_bar:
                bar.update(number_of_dumped_keys)
            self.__file.write("\n")
        if self._enable_progress_bar:
            bar.finish()
        if self._log:
            print(f"Number of Dumped Keys: {number_of_dumped_keys}", file=sys.stderr)

    def restore(self, io: RedisIO):
        for line in self.__file:
            if len(line) > 0:
                j = json.loads(line)
                io.write(j["key"], j["type"], j["value"], j["ttl"])
        io.flush()


class CSVDumper(DataDumper):
    def __init__(
        self,
        f: IO,
        preserve_ttls: bool = False,
        enable_progress_bar: bool = True,
        log: bool = True
    ) -> None:
        self.__file = f
        self._preserve_ttls = preserve_ttls
        self._enable_progress_bar = enable_progress_bar
        self._log = log

    def dump(self, io: RedisIO):
        writer = csv.writer(self.__file, lineterminator="\n")
        if self._enable_progress_bar:
            total_keys = io.count_keys()
            bar = progressbar.ProgressBar(maxval=total_keys, \
                widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
            bar.start()
        
        number_of_dumped_keys = 0
        for _type, key, val, ttl in io:
            _ttl = ttl if self._preserve_ttls else -1
            writer.writerow([key, _type, val, _ttl])
            number_of_dumped_keys += 1
            if self._enable_progress_bar:
                bar.update(number_of_dumped_keys)
        if self._enable_progress_bar:
            bar.finish()
        if self._log:
            print(f"Number of Dumped Keys: {number_of_dumped_keys}", file=sys.stderr)


    def restore(self, io: RedisIO):
        reader = csv.reader(self.__file, lineterminator="\n")
        for key, _type, val, ttl in reader:
            io.write(key, _type, val, ttl)
        io.flush()
