from .abstract_deployer import AbstractDeployer
import uuid
import subprocess
import os

def _execute(host, cmd, error_ok=False):
    r = subprocess.call(["ssh", "-q", "-t", host, cmd], shell=False)
    if not error_ok and r != 0:
        raise Exception(f"command {cmd} failed with exit code {r}")


class HDFSDeployer(AbstractDeployer):
    """
    This deployer deploys application to HDFS
    """
    def __init__(self, config):
        super(HDFSDeployer, self).__init__(config)

    def deploy(self, build_dir, deployment_location):
        # let's copy files to the stage dir
        bridge_dir = os.path.join(self.config['stage_dir'], str(uuid.uuid4()))

        bridge = self.config["bridge"]
        _execute(bridge, f"mkdir -p {bridge_dir}")

        subprocess.call([
            'scp', '-q', f"{build_dir}/app.zip", f"{bridge}:{bridge_dir}/app.zip"
        ])
        subprocess.call([
            'scp', '-q', f"{build_dir}/lib.zip", f"{bridge}:{bridge_dir}/lib.zip"
        ])
        subprocess.call([
            'scp', '-q', f"{build_dir}/main.py", f"{bridge}:{bridge_dir}/main.py"
        ])

        _execute(bridge, f"hdfs dfs -rm -r {deployment_location}", error_ok=True)
        _execute(bridge, f"hdfs dfs -mkdir -p {deployment_location}")
        _execute(bridge, f"hdfs dfs -copyFromLocal {bridge_dir}/app.zip {deployment_location}/app.zip")
        _execute(bridge, f"hdfs dfs -copyFromLocal {bridge_dir}/lib.zip {deployment_location}/lib.zip")
        _execute(bridge, f"hdfs dfs -copyFromLocal {bridge_dir}/main.py {deployment_location}/main.py")
        _execute(bridge, f"rm -rf {bridge_dir}")