class AbstractDeployer:
    def __init__(self, config):
        self.config = config

    def deploy(self, build_dir, deployment_location):
        raise NotImplementedError()