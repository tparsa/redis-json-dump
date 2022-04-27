from rediscluster import RedisCluster
import json

from single_redis import SingleRedisPumper


class RedisClusterPumper(SingleRedisPumper):
    def __init__(self, startup_nodes, password):
        self.r = RedisCluster(startup_nodes=startup_nodes, decode_responses=True, password=password)
