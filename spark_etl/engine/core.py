import uuid

class DataDescriptor(object):
    def __init__(self, namespace):
        self.namespace = namespace


class DataObject(object):
    def __init__(self, driver, data_descriptor=None):
        self.driver = driver
        self.data_descriptor = data_descriptor

    def load(self):
        raise NotImplementedError()

    def show_snippet(self):
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
    
