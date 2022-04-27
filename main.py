from single_redis import SingleRedisDumper, SingleRedisPumper
from redis_cluster import RedisClusterDumper, RedisClusterPumper

if __name__ == "__main__":
    cluster_nodes = [{'host': '172.18.0.7', 'port': 6379}, {'host': '172.18.0.3', 'port': 6379}, {'host': '172.18.0.4', 'port': 6379}]
    RedisClusterDumper(cluster_nodes, password='bitnami').dump_all()
    x = input()
    RedisClusterPumper(cluster_nodes, password='bitnami').pump_all()
    # SingleRedisDumper('redis://localhost:6379').dump_all()
    # SingleRedisPumper('redis://localhost:6379/1').pump_with_pattern('*')
