class ServerChannelInterface:
    def read_json(self, spark, name):
        raise NotImplementedError()


    def has_json(self, spark, name):
        raise NotImplementedError()


    def write_json(self, spark, name, payload):
        raise NotImplementedError()


    def delete_json(self, spark, name):
        raise NotImplementedError()


class ClientChannelInterface:
    def read_json(self, name):
        raise NotImplementedError()


    def write_json(self, name, payload):
        raise NotImplementedError()


    def has_json(self, name):
        raise NotImplementedError()


    def delete_json(self, name):
        raise NotImplementedError()
