class Valve(object):
    def __init__(self):
        # key is name of the port, value is instance of ValvePort
        self.__ip_dict = {}
        # key is the name of port, value is the instance of the ValvePort
        self.__op_dict = {}
    
    @property
    def ip_dict(self):
        return self.__ip_dict
    
    @property
    def op_dict(self):
        return self.__op_dict
    
    def execute(self):
        raise NotImplementedError()


class ValvePort(object):
    def __init__(self, owner, is_input):
        self.owner = owner
        self.is_input = is_input
        self.connected_port = None
        self.data_queue = []

    def connect(self, other):
        if self.is_input or (not other.is_input):
            raise Exception("Only input port can connect to output port")
        self.connected_port = other
    
    def queue_data(self, data):
        self.data_queue.append(data)
        self.owner.execute()

    def emit(self, data):
        self.connected_port.queue_data(data)



