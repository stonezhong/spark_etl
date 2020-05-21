from .base import Driver


class SQLDriver(Driver):
    def __init__(self, config=None):
        super(SQLDriver, self).__init__(config=config)
    
    def connect(self):
        raise NotImplementedError()
    
    def copy_to(self, do_from, do_to):
        raise NotImplementedError()

    def append_to(self, do_from, do_to):
        raise NotImplementedError()


