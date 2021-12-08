import os
from urllib.parse import urlparse
import json
import tempfile

from .main import ClientChannelInterface

"""Client channel using S3 file system
"""
class HDFSClientChannel(ClientChannelInterface):
    def __init__(self, bridge, stage_dir, run_dir, run_id, ssh_config):
        self.bridge = bridge        # the bridge server's name which we can ssh to
        self.stage_dir = stage_dir  # stage_dir is on bridge
        self.run_dir = run_dir      # base dir for all runs, e.g. hdfs:///beta/etl/runs
        self.run_id  = run_id       # string, unique id of the run
        self.ssh_config = ssh_config

    def _create_stage_dir(self):
        # We are using stage_dir instead of temp dir to stage objects on bridge
        # the reason is, many cases temp dir is in memory and cannot hold large object
        self.ssh_config.execute(self.bridge, [
            "mkdir", "-p", os.path.join(self.stage_dir, self.run_id)
        ])


    def read_json(self, name):
        """read a json object from HDFS run dir with specific name
        """
        self._create_stage_dir()
        src  = os.path.join(self.stage_dir, self.run_id, name)
        self.ssh_config.execute(self.bridge, [
            "hdfs", "dfs", "-copyToLocal", "-f",
            os.path.join(self.run_dir, self.run_id, name),
            src
        ])

        try:
            stage_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
            stage_file.close()
            self.ssh_config.scp(f"{self.bridge}:{src}", stage_file.name)
            with open(stage_file.name) as f:
                return json.load(f)
        finally:
            os.remove(stage_file.name)


    def has_json(self, name):
        """check if a file with specific name exist or not in the HDFS run dir
        """
        exit_code = self.ssh_config.execute(self.bridge, [
            "hdfs", "dfs", "-test", "-f",
            os.path.join(self.run_dir, self.run_id, name)
        ], error_ok=True)
        if exit_code == 0:
            return True
        if exit_code == 1:
            return False
        raise Exception(f"Unrecognized exit code: {exit_code}")

    def write_json(self, name, payload):
        """write a json object to HDFS run dir with specific name
        """
        self._create_stage_dir()

        stage_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        stage_file.close()
        try:
            with open(stage_file.name, "wt") as f:
                json.dump(payload, f)

            # copy over to bridge at stage directory
            dest = os.path.join(self.stage_dir, self.run_id, name)
            self.ssh_config.scp(stage_file.name, f"{self.bridge}:{dest}")

            # then upload to HDFS, -f for overwrite if exist
            self.ssh_config.execute(self.bridge, [
                "hdfs", "dfs", "-copyFromLocal", "-f",
                dest,
                os.path.join(self.run_dir, self.run_id, name)
            ])
        finally:
            os.remove(stage_file.name)

    def delete_json(self, name):
        self.ssh_config.execute(self.bridge, [
            "hdfs", "dfs", "-rm",
            os.path.join(self.run_dir, self.run_id, name)
        ])
