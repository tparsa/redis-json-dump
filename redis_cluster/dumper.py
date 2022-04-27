from base64 import decode
from rediscluster import RedisCluster

from single_redis import SingleRedisDumper


class RedisClusterDumper(SingleRedisDumper):
    def __init__(self, startup_nodes, password):
        self.r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, password=password)
        self._first_dump_line = True

    def dump_with_pattern(self, pattern):
        with open('dump.json', 'w') as dump_file:
            dump_file.write('{\n')
        cursor = 0
        keys = []
        for key in self.r.scan_iter(match=pattern):
            keys.append(key)
            if len(keys) > 10:
                self.validate_keys(keys)
                self.dump_keys(keys)
                keys = []
        if len(keys):
            self.validate_keys(keys)
            self.dump_keys(keys)
        with open('dump.json', 'a') as dump_file:
            dump_file.write('\n}')