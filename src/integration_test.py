import sys
import time

from src.integration_tests import (
    single_redis_test,
    cluster_redis_test,
    s3_dumper_test
)

count_foreach = 10
if len(sys.argv) > 1:
    count_foreach = int(sys.argv[1])

single_redis_test(
    count_foreach=count_foreach,
    source_redis_url="redis://redis_source/0",
    dest_redis_url="redis://redis_dest/0"
).test()

print("single redis integration test completed", file=sys.stderr)

time.sleep(5)

cluster_redis_test(
    count_foreach=count_foreach,
    source_redis_url="redis://173.18.0.2/0",
    dest_redis_url="redis://173.18.0.6/0"
).test()

print("redis cluster integration test completed", file=sys.stderr)

s3_dumper_test(
    count_foreach=count_foreach,
    source_redis_url="redis://redis_source/0",
    dest_redis_url="redis://redis_dest_s3/0",
    s3_endpoint_url="http://minio:9000"
).test()

print("s3 dumper integration test completed", file=sys.stderr)