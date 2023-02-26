import os
import sys
import subprocess
import gzip
import shutil
import boto3
from redis import Redis

from src.mock.args import ArgsMock
from src.integration_tests.base import TestBase
from src.integration_tests.utils import *

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


class Test(TestBase):
    def __init__(
        self,
        count_foreach,
        source_redis_url,
        dest_redis_url,
        s3_endpoint_url
    ):
        self.setUpClass(
            count_foreach,
            source_redis_url,
            dest_redis_url,
            s3_endpoint_url
        )
        self.keys = []

    def setUpClass(
        self,
        count_foreach,
        source_redis_url,
        dest_redis_url,
        s3_endpoint_url
    ):
        self.count_foreach = count_foreach
        self.source_redis = Redis.from_url(source_redis_url)
        self.source_redis.flushall()
        self.dest_redis = Redis.from_url(dest_redis_url)
        self.dest_redis.flushall()

        self.keys = []

        for type in TEST_TYPES:
            for i in range(count_foreach):
                key = f"{KEY_PREFIX}:{type}:{i}"
                HANDLERS[type].write(self.source_redis, key, VALUE_GENERATOR[type](), -1)
                self.keys.append(key)
        
        self.dump_args = ArgsMock()
        self.dump_args.add("uri", source_redis_url)
        self.dump_args.add("type", "single")
        self.dump_args.add("db", 0)
        self.dump_args.add("ttl", True)
        self.dump_args.add("bucket", "test")
        self.dump_args.add("endpoint_url", s3_endpoint_url)
        self.dump_args.add("name", "test")
        self.dump_args.add("keep-last", 2)

        self._restore_file = "dump_single_redis_s3_integration_test.json"
        self.restore_args = ArgsMock()
        self.restore_args.add("uri", dest_redis_url)
        self.restore_args.add("input_stdin", False)
        self.restore_args.add("file", self._restore_file)
        self.restore_args.add("mode", "restore")
        self.restore_args.add("type", "single")
        self.restore_args.add("db", 0)
        self.restore_args.add("ttl", True)

        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=s3_endpoint_url
        )
        s3_client.create_bucket(
            Bucket=self.dump_args.bucket
        )
    
    def test(self):
        self._dump()
        
        self._check_s3_backup_status(
            endpoint_url=self.dump_args.endpoint_url,
            bucket=self.dump_args.bucket,
            prefix_name=self.dump_args.name,
            expected_objects_count=1
        )

        self._restore_backup(
            endpoint_url=self.dump_args.endpoint_url,
            bucket=self.dump_args.bucket,
            prefix_name=self.dump_args.name,
        )

        self._check_integrity()

        self._dump()
        self._check_s3_backup_status(
            endpoint_url=self.dump_args.endpoint_url,
            bucket=self.dump_args.bucket,
            prefix_name=self.dump_args.name,
            expected_objects_count=2
        )

        self._dump()
        self._check_s3_backup_status(
            endpoint_url=self.dump_args.endpoint_url,
            bucket=self.dump_args.bucket,
            prefix_name=self.dump_args.name,
            expected_objects_count=2
        )
        print("redis s3 dump integrity checked successfully.", file=sys.stderr)

    def _dump(self):
        dump_command = f"python -m src.s3_dumper {' '.join(map(str,self.dump_args.argv()))}"
        dump_process = subprocess.Popen(
            dump_command,
            shell=True,
            env=os.environ.copy()
        )
        dump_process.wait()
        assert dump_process.returncode == 0

    def _check_s3_backup_status(
        self,
        endpoint_url,
        bucket,
        prefix_name,
        expected_objects_count
    ):
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=endpoint_url
        )

        response = s3_client.list_objects(
            Bucket=bucket,
            Prefix=prefix_name
        )

        assert len(response['Contents']) == expected_objects_count

    def _restore_backup(
        self,
        endpoint_url,
        bucket,
        prefix_name
    ):
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            endpoint_url=endpoint_url
        )

        response = s3_client.list_objects(
            Bucket=bucket,
            Prefix=prefix_name
        )

        key = response['Contents'][0]['Key']
        file_name = f"{self._restore_file}.gz"
        s3_client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=file_name
        )

        self._gunzip(file_name)

        from ..main import CLI
        CLI(self.restore_args).execute()
    
    def _gunzip(self, file_name):
        with gzip.open(file_name, "rb") as f_in:
            with open(file_name[:-3], "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
