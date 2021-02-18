import subprocess
import os
import shutil


from spark_etl.deployers import AbstractDeployer
from spark_etl import Build


class LocalDeployer(AbstractDeployer):
    """
    This deployer deploys application to local file system
    """
    def __init__(self, config):
        super(LocalDeployer, self).__init__(config)

    def deploy(self, build_dir, deployment_location):
        # directory layout
        # deployment_location
        #   |
        #   +-- {build_version}
        #         |
        #         +-- app.zip
        #         |
        #         +-- job_loader.py
        #         |
        #         +-- lib
        #         |
        #         +-- main.py
        #         |
        #         +-- manifest.json

        build = Build(build_dir)

        # copy artifacts
        target_dir = os.path.join(deployment_location, build.version)
        if os.path.isdir(target_dir):
            shutil.rmtree(target_dir)
        os.makedirs(target_dir)
        for artifact in build.artifacts:
            src = os.path.join(build_dir, artifact)
            dst = os.path.join(target_dir, artifact)
            print(f"Copy from {src} to {dst}")
            shutil.copyfile(src, dst)

        src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "job_loader.py")
        dst = os.path.join(target_dir, "job_loader.py")
        shutil.copyfile(src, dst)
        print(f"Copy from {src} to {dst}")

        print("Deployment is done")




