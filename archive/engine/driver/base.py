class Driver(object):
    def __init__(self, config=None):
        self.config = config
    
    def connect(self):
        raise NotImplementedError()

    def load_do(self, do):
        raise NotImplementedError()

    def save_do(self, do):
        raise NotImplementedError()

