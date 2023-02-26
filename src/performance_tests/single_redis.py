import sys
import subprocess
from datetime import datetime
from redis import Redis

from src.integration_tests.utils import *
from src.mock.args import ArgsMock


class Test:
    def __init__(
        self,
        count_foreach,
        source_redis_url,
        dest_redis_url,
        new_redis_url,
        source_redis_container_name,
        dest_redis_container_name
    ):
        self.setUpClass(
            count_foreach,
            source_redis_url,
            dest_redis_url,
            new_redis_url,
            source_redis_container_name,
            dest_redis_container_name
        )

    def setUpClass(
        self,
        count_foreach,
        source_redis_url,
        dest_redis_url,
        new_redis_url,
        source_redis_container_name,
        dest_redis_container_name
    ):
        self.count_foreach = count_foreach
        self.source_redis = Redis.from_url(source_redis_url)
        self.source_redis.flushall()
        self.dest_redis = Redis.from_url(dest_redis_url)
        self.dest_redis.flushall()
        self._new_redis_url = new_redis_url

        self._source_redis_container_name = source_redis_container_name
        self._dest_redis_container_name = dest_redis_container_name

        self.keys = []

        for type in TEST_TYPES:
            for i in range(count_foreach):
                key = f"{KEY_PREFIX}:{type}:{i}"
                HANDLERS[type].write(self.source_redis, key, VALUE_GENERATOR[type](), -1)
                self.keys.append(key)

        restore_file = "dump_single_redis_integration_test.json"

        self.dump_args = ArgsMock()
        self.dump_args.add("uri", source_redis_url)
        self.dump_args.add("output_stdout", False)
        self.dump_args.add("file", restore_file)
        self.dump_args.add("mode", "dump")
        self.dump_args.add("type", "single")
        self.dump_args.add("db", 0)
        self.dump_args.add("ttl", True)

        self.restore_args = ArgsMock()
        self.restore_args.add("uri", dest_redis_url)
        self.restore_args.add("input_stdin", False)
        self.restore_args.add("file", restore_file)
        self.restore_args.add("mode", "restore")
        self.restore_args.add("type", "single")
        self.restore_args.add("db", 0)
        self.restore_args.add("ttl", True)

    def test(self):
        from ..main import CLI
        start = datetime.now().timestamp()
        CLI(self.dump_args).execute()
        CLI(self.restore_args).execute()
        end = datetime.now().timestamp()
        print(f"dump and restore with tool for {len(self.keys)} took {end - start} seconds", file=sys.stderr)

        start = datetime.now().timestamp()
        self.source_redis.save()
        end = datetime.now().timestamp()
        dump_duration = end - start

        import docker
        docker_cli = docker.from_env()
        source_redis_container = docker_cli.containers.get(self._source_redis_container_name)
        rdb = source_redis_container.get_archive("/data/dump.rdb")
        dest_redis_container = docker_cli.containers.get(self._dest_redis_container_name)
        dest_redis_container.put_archive("/data/", rdb[0])

        start = datetime.now().timestamp()
        dest_redis_container.exec_run("redis-server --dbfilename dump.rdb --dir /data --port 6380 --daemonize yes")
        self._new_redis = Redis.from_url(self._new_redis_url)
        while True:
            try:
                self._new_redis.ping()
                break
            except Exception as e:
                print(e, file=sys.stderr)
                pass
        end = datetime.now().timestamp()
        restore_duration = end - start
        print(f"dump and restore with bgsave for {len(self.keys)} took {dump_duration + restore_duration}", file=sys.stderr)
        subprocess.check_output("ps aux | grep 6380 | grep -v grep | awk '{print $1}' | xargs -I {} kill -9 {}", shell=True)
