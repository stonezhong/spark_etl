from pywebhdfs.webhdfs import PyWebHdfsClient
from .misc import retry

class HDFS(object):
    __MAX_RETRY_COUNT      = 3
    __UPLOAD_CHUNK_SIZE    = 1024*1024

    def __init__(self, service_url, 
                 username=None, password=None, hdfs_username=None, 
                 auth_cert=None, auth_key=None, ca_cert=None
    ):
        if not service_url.endswith("/"):
            service_url += "/"

        if auth_cert is None:
            self._hdfs = PyWebHdfsClient(
                base_uri_pattern=service_url,
                request_extra_opts={
                    'verify': False,
                    'auth': (username, password)
                },
                user_name=hdfs_username
            )
        else:
            self._hdfs = PyWebHdfsClient(
                base_uri_pattern=service_url,
                request_extra_opts={
                    'cert': (auth_cert, auth_key, ),
                    'verify': ca_cert,
                },
                user_name=hdfs_username
            )
    

    def make_dir(self, remote_path):
        retry(
            lambda : self._hdfs.make_dir(remote_path),
            self.__MAX_RETRY_COUNT
        )

    def delete_file_dir(self, remote_path, recursive=False):
        retry(
            lambda : self._hdfs.delete_file_dir(remote_path, recursive=recursive),
            self.__MAX_RETRY_COUNT
        )
    
    def upload_file(self, local_path, remote_path, chunk_size=__UPLOAD_CHUNK_SIZE):
        retry(
            lambda: self._hdfs.create_file(remote_path, b''),
            self.__MAX_RETRY_COUNT
        )

        with open(local_path, "rb") as f:
            while True:
                chunk = f.read(self.__HDFS_UPLOAD_CHUNK_SIZE)
                if len(chunk) > 0:
                    self._hdfs.append_file(remote_path, chunk)
                if len(chunk) < __HDFS_UPLOAD_CHUNK_SIZE:
                    break
