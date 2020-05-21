class AbstractJobSubmitter:
    def __init__(self, config):
        self.config = config

    def run(self, deployment_location):
        """
        run an application
        """
        raise NotImplementedError()
