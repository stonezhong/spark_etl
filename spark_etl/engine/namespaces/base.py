class Namespace(object):
    def __init__(self, name):
        self.name = name

    def create_do(self, path, **kwargs):
        raise NotImplementedError()

    def do(self, path):
        raise NotImplementedError()
