import uuid
import subprocess
import os
from urllib.parse import urlparse

from .abstract_deployer import AbstractDeployer
from spark_etl import Build
from spark_etl.exceptions import SparkETLDeploymentFailure
from spark_etl.ssh_config import SSHConfig


class HDFSDeployer(AbstractDeployer):
    """
    This deployer deploys application to HDFS
    """
    def __init__(self, config):
        super(HDFSDeployer, self).__init__(config)
        self.ssh_config = SSHConfig(config['ssh_config'])

    def deploy(self, build_dir, deployment_location):
        self.ssh_config.generate()
        try:
            o = urlparse(deployment_location)
            if o.scheme != 'hdfs':
                raise SparkETLDeploymentFailure("deployment_location must be in hdfs")

            # let's copy files to the stage dir
            bridge_dir = os.path.join(self.config['stage_dir'], str(uuid.uuid4()))

            bridge = self.config["bridge"]
            self.ssh_config.execute(bridge, ["mkdir", "-p", bridge_dir])

            build = Build(build_dir)

            for artifact in build.artifacts:
                self.ssh_config.scp(
                    f"{build_dir}/{artifact}", f"{bridge}:{bridge_dir}/{artifact}"
                )

            # copy job loader
            job_loader_filename = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'job_loader.py'
            )
            self.ssh_config.scp(job_loader_filename, f"{bridge}:{bridge_dir}/job_loader.py")

            dest_location = f"{deployment_location}/{build.version}"
            self.ssh_config.execute(bridge, ["hdfs", "dfs", "-rm", "-r", dest_location], error_ok=True)
            self.ssh_config.execute(bridge, ["hdfs", "dfs", "-mkdir", "-p", dest_location])

            artifacts = []
            artifacts.extend(build.artifacts)
            artifacts.append("job_loader.py")
            for artifact in artifacts:
                self.ssh_config.execute(bridge, [
                    "hdfs", "dfs", "-copyFromLocal", f"{bridge_dir}/{artifact}", f"{dest_location}/{artifact}"
                ])

            self.ssh_config.execute(bridge, ["rm", "-rf", bridge_dir])
        finally:
            self.ssh_config.destroy()
