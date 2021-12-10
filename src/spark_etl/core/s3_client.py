import os
from urllib.parse import urlparse
import json
import tempfile

from .main import ClientChannelInterface

import boto3
from botocore.exceptions import ClientError

"""Client channel using S3 file system
"""
class S3ClientChannel(ClientChannelInterface):
    def __init__(self, run_home_dir, aws_access_key_id, aws_secret_access_key):
        self.run_home_dir = run_home_dir
        self.s3_client = boto3.client(
            's3', 
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key
        )
        o = urlparse(run_home_dir)
        assert o.scheme in ('s3', 's3a')
        self.bucket_name = o.netloc
        self.s3_dirname = os.path.join(o.path[1:]) # remove the leading '/'


    def read_json(self, name):
        stage_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        stage_file.close()
        key = os.path.join(self.s3_dirname, name)
        try:
            self.s3_client.download_file(self.bucket_name, key, stage_file.name)
            with open(stage_file.name) as f:
                return json.load(f)
        finally:
            os.remove(stage_file.name)


    def has_json(self, name):
        try:
            key = os.path.join(self.s3_dirname, name)
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False

    def write_json(self, name, payload):
        stage_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        stage_file.close()
        key = os.path.join(self.s3_dirname, name)
        try:
            with open(stage_file.name, "w") as f:
                json.dump(payload, f)
            self.s3_client.upload_file(stage_file.name, self.bucket_name, key)
        finally:
            os.remove(stage_file.name)

    def delete_json(self, name):
        try:
            key = os.path.join(self.s3_dirname, name)
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] != "404":
                raise

