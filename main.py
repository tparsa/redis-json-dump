import sys
from io import StringIO

from lib.dumpers import JSONDumper
from lib.io import RedisClusterIO, RedisSingleIO


if __name__ == "__main__":
    f = StringIO()

    # s = RedisSingleIO("redis://localhost:6379/0")
    s = RedisClusterIO("redis://:bitnami@172.18.0.3:6379")
    backup = JSONDumper(f)
    backup.dump(s)

    input()

    f.seek(0)
    backup.restore(s)
