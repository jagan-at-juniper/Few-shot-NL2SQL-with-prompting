from abc import ABC, abstractmethod


class CloudStorageClient(ABC):

    @abstractmethod
    def is_path_exists(self, fs_bucket, fs_path):
        pass

    @abstractmethod
    def upload_file(self, local_file_path, fs_bucket, fs_path):
        pass

    @abstractmethod
    def upload_fileobject(self, file_obj, fs_bucket, fs_path):
        pass
