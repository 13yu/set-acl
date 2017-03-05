import json
import time
import urlparse

import boto3
from botocore.client import Config


def get_client(access_key, secret_key, endpoint_url):
    config = Config(signature_version='s3v4')
    client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=config,
        region_name='us-east-1',
        endpoint_url=endpoint_url,
    )
    return client


def get_all_files(client, bucket_name, prefix='', marker=''):

    all_files = []
    while True:
        resp = client.list_objects(
            Bucket=bucket_name,
            Prefix=prefix,
            Marker=marker,
        );

        if 'Contents' not in resp:
            break

        for content in resp['Contents']:
            all_files.append(content['Key'])

        marker = all_files[-1]

    return all_files


def set_files_acl(client, bucket_name, files):
    for file in files:
        resp = client.put_object_acl(
            Bucket=bucket_name,
            Key=file,
            ACL='public-read',
        )
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            print 'failed to set acl for file: ' + file


if __name__ == '__main__':
    access_key = 'ziw5dp1alvty9n47qksu'
    secret_key = 'V+ZTZ5u5wNvXb+KP5g0dMNzhMeWe372/yRKx4hZV'
    bucket_name = 'renzhi-test-bucket'
    acl = 'public-read'

    client = get_client(access_key, secret_key, 'http://bscstorage.com')
    all_files = get_all_files(client, bucket_name)

    print 'about to change acl of %d files' % len(all_files)

    set_files_acl(client, bucket_name, all_files)
