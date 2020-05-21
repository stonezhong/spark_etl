from .base import Namespace
from ..core import DataObject

class HDFSDataObject(DataObject):
    def __init__(self, namespace, path, format):
        super(HDFSDataObject, self).__init__(namespace, path, format)
    
    def to_df(self, **kwargs):
        return self.namespace.to_df(self.path, self.format, **kwargs)


class HDFSNamespace(Namespace):
    def __init__(self, name, spark):
        super(HDFSNamespace, self).__init__(name)
        self.spark = spark

    def get_data_object(self, path, format):
        return HDFSDataObject(self, path, format)
    
    def to_df(self, path, format, **kwargs):
        if format == 'parquet':
            return self.spark.read.parquet(path)
        raise ValueError("format {} is not supported".format(format))
