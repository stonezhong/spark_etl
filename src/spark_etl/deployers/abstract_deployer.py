class AbstractDeployer:
    def __init__(self, config):
        self.config = config

    def deploy(self, build_dir, destination_location):
        raise NotImplementedError()

