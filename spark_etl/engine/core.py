import uuid

class DataObject(object):
    def __init__(self, namespace, path):
        self.namespace = namespace
        self.path = path

    def to_df(self):
        raise NotImplementedError()

    def download(self, local_filename):
        raise NotImplementedError()




class ETLEngine(object):
    def __init__(self):
        self.namespace_dict = {}
        self.node_dict = {} # key is node id, value is node

    def register_namespace(self, namespace):
        self.namespace_dict[namespace.name] = namespace
    
    def get_namespace(self, name):
        return self.namespace_dict.get(name)

    def get_data_object(self, namespace_name, path, format):
        return self.namespace_dict[namespace_name].get_data_object(path, format)
    
