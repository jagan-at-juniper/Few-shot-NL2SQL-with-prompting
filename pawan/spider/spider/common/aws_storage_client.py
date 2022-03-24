
import logging
import boto3


from common.cloud_storage_client import CloudStorageClient


class S3StorageClient(CloudStorageClient):

    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')

    @staticmethod
    def parse_fs_status(resp):
        if resp and resp.get('ResponseMetadata') and resp.get('ResponseMetadata').get('HTTPStatusCode') == 200:
            return True
        else:
            return False

    def is_path_exists(self, fs_bucket, fs_path):
        resp = self.s3_client.list_objects_v2(Bucket=fs_bucket,
                                              Prefix=fs_path)
        return resp.get('KeyCount') > 0

    def upload_file(self, file_path, bucket, destination_path):
        resp = self.s3_client.upload_file(file_path, bucket, destination_path)
        logging.info(f"Uploading file from {file_path} to s3://{bucket}/{destination_path}")
        return self.parse_fs_status(resp)

    def upload_fileobject(self, file_obj, bucket, destination_path):
        resp = self.s3_client.upload_fileobj(file_obj, bucket, destination_path)
        logging.info(f"Uploading file object to s3://{bucket}/{destination_path}")
        return self.parse_fs_status(resp)
