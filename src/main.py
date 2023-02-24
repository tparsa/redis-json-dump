import sys
import os
import argparse

from src.redis_lib.dumpers import JSONDumper
from src.redis_lib.io import RedisClusterIO, RedisSingleIO


class CLI:
    def __init__(self, args):
        self._args = args
        self._uri = f"{self._get_redis_uri()}/{self._args.db}"

    def _execute_single_redis(self, f, mode, ttl):
        r = RedisSingleIO(self._uri)
        backup = JSONDumper(f, ttl)
        getattr(backup, mode)(r)
    
    def _execute_cluster_redis(self, f, mode, ttl):
        r = RedisClusterIO(self._uri)
        backup = JSONDumper(f, ttl)
        getattr(backup, mode)(r)

    def execute(self):
        args = self._args
        mode = args.mode
        f = None
        if mode == 'dump':
            if args.output_stdout:
                f = sys.stdout
            elif args.file:
                f = open(args.file, 'w')
            else:
                raise Exception("In dump mode either set --output-stdout or give a file with --file")
        else:
            if args.input_stdin:
                f = sys.stdin
            elif args.file:
                f = open(args.file, 'r')
            else:
                raise Exception("In restore mode either set --input-stdin or give a file with --file")
        
        getattr(self, f"_execute_{args.type}_redis")(f, args.mode, args.ttl)

    def _get_redis_uri(self):
        uri_env = os.getenv("uri", None)
        if uri_env:
            return uri_env
        elif self._args.uri:
            return self._args.uri
        raise Exception("Either set uri environment variable or pass uri as argument")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Dump redis with different types')
    parser.add_argument('--uri', '-U', nargs='?', default=None)
    parser.add_argument('--output-stdout', action='store_true')
    parser.add_argument('--input-stdin', action='store_true')
    parser.add_argument('--file', '-F', nargs='?', default="dump.json")
    parser.add_argument('--mode', '-M', required=True, choices=['dump', 'restore'])
    parser.add_argument('--type', '-T', required=True, choices=['single', 'cluster'])
    parser.add_argument('--db', '-D', nargs='?', default=0)
    parser.add_argument('--ttl', action='store_true')

    args = parser.parse_args()

    cli = CLI(args)
    cli.execute()
