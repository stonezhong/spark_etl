import uuid
import subprocess
import os
from urllib.parse import urlparse

from .abstract_deployer import AbstractDeployer
from spark_etl import Build
from spark_etl.exceptions import SparkETLDeploymentFailure
from spark_etl.ssh_config import SSHConfig
import spark_etl


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
            if o.scheme not in ('hdfs', 's3', 's3a'):
                raise SparkETLDeploymentFailure("deployment_location must be in hdfs, s3 or s3a")

            # let's copy files to the stage dir
            bridge_dir = os.path.join(self.config['stage_dir'], str(uuid.uuid4()))

            bridge = self.config["bridge"]
            self.ssh_config.execute(bridge, ["mkdir", "-p", bridge_dir])

            build = Build(build_dir)

            src_file_loc = {}
            for artifact in build.artifacts:
                src = os.path.join(build_dir, artifact)
                dest = os.path.join(bridge_dir, artifact)
                src_file_loc[artifact] = src
                self.ssh_config.scp(src,f"{bridge}:{dest}")

            # copy job loader
            spark_etl_dir = os.path.dirname(os.path.abspath(spark_etl.__file__))
            src = os.path.join(spark_etl_dir, 'core', 'loader_util', 'resources', 'job_loader.py')
            src_file_loc['job_loader.py'] = src
            dest = os.path.join(bridge_dir, 'job_loader.py')
            self.ssh_config.scp(src, f"{bridge}:{dest}")

            dest_location = os.path.join(deployment_location, build.version)
            self.ssh_config.execute(bridge, ["hdfs", "dfs", "-rm", "-r", dest_location], error_ok=True)
            self.ssh_config.execute(bridge, ["hdfs", "dfs", "-mkdir", "-p", dest_location])

            artifacts = []
            artifacts.extend(build.artifacts)
            artifacts.append("job_loader.py")
            for artifact in artifacts:
                src = src_file_loc[artifact]
                dest = os.path.join(dest_location, artifact)
                print(f"{src}  ==> {dest}")
                self.ssh_config.execute(bridge, [
                    "hdfs", "dfs", "-copyFromLocal", os.path.join(bridge_dir, artifact), dest
                ])

            self.ssh_config.execute(bridge, ["rm", "-rf", bridge_dir])
        finally:
            self.ssh_config.destroy()
