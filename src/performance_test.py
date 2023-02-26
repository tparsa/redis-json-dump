import sys
import time

from src.performance_tests import (
    single_redis_test
)

base = 10
max_power = 5
if len(sys.argv) > 1:
    max_power = int(sys.argv[1])

kwargs = {
    "count_foreach": base,
    "source_redis_url": "redis://redis_source/0",
    "dest_redis_url": "redis://redis_dest/0",
    "new_redis_url": "redis://redis_dest:6380/0",
    "source_redis_container_name": "redis-json-dump-redis_source-1",
    "dest_redis_container_name": "redis-json-dump-redis_dest-1"
}

redis_test = single_redis_test(**kwargs)

count_foreach = base
for i in range(max_power):
    redis_test.test()
    print(f"single redis performance test with {len(redis_test.keys)} completed.", file=sys.stderr)
    if i == max_power - 1:
        break
    count_foreach *= base
    kwargs["count_foreach"] = count_foreach
    redis_test.setUpClass(**kwargs)

print("single redis performance test completed", file=sys.stderr)
