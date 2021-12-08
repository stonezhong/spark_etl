import subprocess
import os
import shutil


from spark_etl.deployers import AbstractDeployer
from spark_etl import Build
import spark_etl

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
            for fname in os.listdir(target_dir):
                fullname = os.path.join(target_dir, fname)
                if os.path.isdir(fullname):
                    shutil.rmtree(fullname)
                else:
                    os.remove(fullname)
        else:
            os.makedirs(target_dir)
        for artifact in build.artifacts:
            src = os.path.join(build_dir, artifact)
            dst = os.path.join(target_dir, artifact)
            print(f"{src}  ==> {dst}")
            shutil.copyfile(src, dst)

        spark_etl_dir = os.path.dirname(os.path.abspath(spark_etl.__file__))
        src = os.path.join(spark_etl_dir, 'core', 'loader_util', 'resources', 'job_loader.py')
        dst = os.path.join(target_dir, "job_loader.py")
        print(f"{src}  ==> {dst}")
        shutil.copyfile(src, dst)
