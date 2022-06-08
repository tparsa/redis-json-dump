import os
import subprocess
import argparse

# You should set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Dump redis with different types')
    parser.add_argument('--uri', '-U', nargs='?', default=None)
    parser.add_argument('--type', '-T', required=True, choices=['single', 'cluster'])
    parser.add_argument('--db', '-D', nargs='?', default=0)
    parser.add_argument('--ttl', action='store_true')
    parser.add_argument('--bucket', '-B', required=True)
    parser.add_argument('--endpoint-url', required=True)
    parser.add_argument('--name', required=True)

    args = parser.parse_args()
    uri = args.uri if args.uri else os.getenv("REDIS_URI")
    if not uri:
        print("Must give --uri or set REDIS_URI env")
        exit(1)
    command = f"python3 main.py --uri {uri} --type {args.type} --db {args.db} --output-stdout --mode dump"
    if args.ttl:
        command += " --ttl"
    dumper_process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    object_storage_address = f"s3://{args.bucket}/{args.name}"
    aws_process = subprocess.Popen(
        f"aws s3 cp - {object_storage_address} --endpoint-url {args.endpoint_url} --expected-size 109261619200",
        shell=True,
        stdin=dumper_process.stdout
    )
    dumper_process.wait()
    aws_process.wait()
