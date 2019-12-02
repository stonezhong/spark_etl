class Namespace(object):
    def __init__(self, name):
        self.name = name
    
    def create_data_descriptor(self, path, **kwargs):
        raise NotImplementedError()
