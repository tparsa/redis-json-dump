from redis.cluster import RedisCluster

from .base import TestBase
from ..mock.args import ArgsMock
from .utils import *


class Test(TestBase):
    def setUpClass(self, count_foreach, source_redis_url, dest_redis_url):
        self.count_foreach = count_foreach
        self.source_redis = RedisCluster.from_url(source_redis_url)
        self.source_redis.flushall()
        self.dest_redis = RedisCluster.from_url(dest_redis_url)
        self.dest_redis.flushall()

        self.keys = []

        for type in TEST_TYPES:
            for i in range(count_foreach):
                key = f"{KEY_PREFIX}:{type}:{i}"
                HANDLERS[type].write(self.source_redis, key, VALUE_GENERATOR[type](), -1)
                self.keys.append(key)


        restore_file = "dump_cluster_redis_integration_test.json"

        self.dump_args = ArgsMock()
        self.dump_args.add("uri", source_redis_url)
        self.dump_args.add("output_stdout", False)
        self.dump_args.add("file", restore_file)
        self.dump_args.add("mode", "dump")
        self.dump_args.add("type", "cluster")
        self.dump_args.add("db", 0)
        self.dump_args.add("ttl", True)

        self.restore_args = ArgsMock()
        self.restore_args.add("uri", dest_redis_url)
        self.restore_args.add("input_stdin", False)
        self.restore_args.add("file", restore_file)
        self.restore_args.add("mode", "restore")
        self.restore_args.add("type", "cluster")
        self.restore_args.add("db", 0)
        self.restore_args.add("ttl", True)