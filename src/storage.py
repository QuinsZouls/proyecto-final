import os
from minio import Minio
MINIO_SERVER = os.getenv('MINIO_SERVER', "localhost:9000")
MINIO_USER = os.getenv('MINIO_USER', "storage")
MINIO_PASSWORD = os.getenv('MINIO_PASSWORD', "password")

client = Minio(MINIO_SERVER, access_key=MINIO_USER,
               secret_key=MINIO_PASSWORD, secure=False)


def initWorkspace():
    if not client.bucket_exists("input"):
        client.make_bucket("input")
    if not client.bucket_exists('output'):
        client.make_bucket("output")
    if not client.bucket_exists('result'):
        client.make_bucket("result")


def countItemsPath(bucket, prefix):
    objects = client.list_objects(bucket, prefix, recursive=True)
    count = 0
    for obj in objects:
        count += 1
    return count
