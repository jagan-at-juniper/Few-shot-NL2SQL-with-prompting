import logging

from google.cloud import storage

from common.cloud_storage_client import CloudStorageClient


class GcpStorageClient(CloudStorageClient):

    def __init__(self):
        self.storage_client = storage.Client()

    def is_path_exists(self, bucket, file_path):
        bucket = self.storage_client.bucket(bucket)
        file_iterator = bucket.list_blobs(prefix=file_path)
        try:
            return any(True for _ in file_iterator)
        except Exception:
            return False

    def upload_file(self, file_path, bucket, destination_path):
        bucket = self.storage_client.get_bucket(bucket)
        blob = bucket.blob(destination_path)
        try:
            blob.upload_from_filename(file_path)
            return True
        except Exception as ex:
            logging.error(f"Error in uploading file from local to storage system::{ex}")
            return False

    def upload_fileobject(self, file_obj, bucket, destination_path):
        bucket = self.storage_client.get_bucket(bucket)
        blob = bucket.blob(destination_path)
        try:
            blob.upload_from_file(file_obj)
            return True
        except Exception as e:
            logging.error(f"Error in uploading file object to storage system::{ex}")
            return False
