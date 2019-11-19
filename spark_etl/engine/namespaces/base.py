class Namespace(object):
    def __init__(self, name):
        self.name = name

    def get_data_object(self, path, format):
        raise NotImplementedError()
