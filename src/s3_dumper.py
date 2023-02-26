import os
import logging

logging.basicConfig(level=int(os.getenv("LOG_LEVEL", logging.INFO)))
logger = logging.getLogger(__name__)

import subprocess
import argparse
from datetime import datetime

import boto3

from src.retention import (
    RetentionBucket,
    always, ymdh, ymd, yw, ym, y
)


# You should set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables


def create_retention_buckets(args):
    return [
        RetentionBucket(count=int(args.keep_last), bucker=always),
        RetentionBucket(count=int(args.keep_hourly), bucker=ymdh),
        RetentionBucket(count=int(args.keep_daily), bucker=ymd),
        RetentionBucket(count=int(args.keep_weekly), bucker=yw),
        RetentionBucket(count=int(args.keep_monthly), bucker=ym),
        RetentionBucket(count=int(args.keep_yearly), bucker=y)
    ]


def prune_single_object_storage_backups(bucket, aws_access_key_id, aws_secret_access_key, s3_endpoint, retention_buckets):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=s3_endpoint
    )

    response = s3_client.list_objects(
        Bucket=bucket
    )

    deleted_backup_keys = []

    for backup in response['Contents'][::-1]:
        key = backup['Key']
        backup_name = key.split('/')[-1].split('.')
        backup_name = '.'.join(backup_name[:2])
        date = datetime.strptime(backup_name, "%Y-%m-%d-%H:%M:%S.%f")
        keep_backup = False
        for retention_bucket in retention_buckets:
            if retention_bucket.count:
                value = retention_bucket.bucker(date)
                if value != retention_bucket.last:
                    retention_bucket.last = value
                    retention_bucket.count -= 1
                    keep_backup = True
        if not keep_backup:
            deleted_backup_keys.append(key)

    logger.info(f"Start deleting {len(deleted_backup_keys)} dumps...")
    for key in deleted_backup_keys:
        logger.info(f"Deleting backup {key}...")
        s3_client.delete_object(
            Bucket=bucket,
            Key=key
        )

    return len(deleted_backup_keys)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Dump redis with different types')
    parser.add_argument('--uri', '-U', nargs='?', default=None)
    parser.add_argument('--type', '-T', required=True, choices=['single', 'cluster'])
    parser.add_argument('--db', '-D', nargs='?', default=0)
    parser.add_argument('--ttl', action='store_true')
    parser.add_argument('--bucket', '-B', required=True)
    parser.add_argument('--endpoint-url', required=True)
    parser.add_argument('--name', required=True)

    parser.add_argument('--keep-last', nargs='?', default=1)
    parser.add_argument('--keep-hourly', nargs='?', default=0)
    parser.add_argument('--keep-daily', nargs='?', default=0)
    parser.add_argument('--keep-weekly', nargs='?', default=0)
    parser.add_argument('--keep-monthly', nargs='?', default=0)
    parser.add_argument('--keep-yearly', nargs='?', default=0)

    args = parser.parse_args()

    uri = args.uri if args.uri else os.getenv("REDIS_URI")
    if not uri:
        print("Must give --uri or set REDIS_URI env")
        exit(1)
    command = f"python3 -m src.main --uri {uri} --type {args.type} --db {args.db} --output-stdout --mode dump"
    if args.ttl:
        command += " --ttl"
    dumper_process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    backup_name = str(datetime.now()).replace(' ', '-')
    object_storage_address = f"s3://{args.bucket}/{args.name}/{backup_name}"
    gzip_process = subprocess.Popen(
        f"gzip",
        shell=True,
        stdin=dumper_process.stdout,
        stdout=subprocess.PIPE
    )
    aws_process = subprocess.Popen(
        f"aws s3 cp - {object_storage_address} --endpoint-url {args.endpoint_url} --expected-size 109261619200",
        shell=True,
        stdin=gzip_process.stdout
    )
    dumper_process.wait()
    gzip_process.wait()
    aws_process.wait()

    total_return_code = dumper_process.returncode | gzip_process.returncode | aws_process.returncode
    if total_return_code == 0:
        logger.info("Backup finished successfully.")
        retention_buckets = create_retention_buckets(args)
        num_of_deleted_backups = prune_single_object_storage_backups(
                                    args.bucket,
                                    os.getenv("AWS_ACCESS_KEY_ID"),
                                    os.getenv("AWS_SECRET_ACCESS_KEY"),
                                    args.endpoint_url,
                                    retention_buckets
                                )
    else:
        logger.error(
            """
            dump failed with dump process return code {}
            and gzip process returncode {}
            store in bucket process returncode {}
            primary s3 storage exit code {}
            """.format(
                dumper_process.returncode, gzip_process.returncode, aws_process.returncode
            )
        )

        aws_command = f"aws s3 rm {object_storage_address}" + f" --endpoint-url {args.endpoint_url}"
        remove_process = subprocess.Popen(aws_command, shell=True)
        remove_process.wait()
