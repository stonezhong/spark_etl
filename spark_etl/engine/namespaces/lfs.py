import os

from .base import Namespace
from ..core import DataDescriptor

class LFSDataDescriptor(DataDescriptor):
    def __init__(self, namespace, path, format):
        super(LFSDataDescriptor, self).__init__(namespace)
        self.path = path
        self.format = format
    
    @property
    def full_path(self):
        return self.namespace.base_dir + self.path

class LFSNamespace(Namespace):
    def __init__(self, name, base_dir):
        super(LFSNamespace, self).__init__(name)
        self.base_dir = base_dir
    
    def create_data_descriptor(self, **kwargs):
        return LFSDataDescriptor(self, kwargs['path'], kwargs['format'])

