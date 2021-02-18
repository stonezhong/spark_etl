import json
import os
import uuid
import tempfile
import subprocess
import json
import time

from spark_etl.job_submitters import AbstractJobSubmitter
from spark_etl.core import ClientChannelInterface
from spark_etl.utils import CLIHandler

class ClientChannel(ClientChannelInterface):
    def __init__(self, run_dir, run_id):
        self.run_dir = run_dir
        self.run_id = run_id

    def _get_json_path(self, name):
        return os.path.join(self.run_dir, self.run_id, name)

    def read_json(self, name):
        with open(self._get_json_path(name), "r") as f:
            return json.load(f)

    def has_json(self, name):
        return os.path.isfile(self._get_json_path(name))

    def write_json(self, name, payload):
        with open(self._get_json_path(name), "w") as f:
            json.dump(payload, f)

    def delete_json(self, name):
        os.remove(self._get_json_path(name))


class PySparkJobSubmitter(AbstractJobSubmitter):
    def __init__(self, config):
        super(PySparkJobSubmitter, self).__init__(config)


    def run(self, deployment_location, options={}, args={}, handlers=[], on_job_submitted=None, cli_mode=False):
        # version is already baked into deployment_location
        # local submitter ignores handlers
        run_id  = str(uuid.uuid4())
        run_dir = self.config['run_dir']
        app_dir = deployment_location

        os.makedirs(os.path.join(run_dir, run_id))

        # generate input.json
        with open(os.path.join(run_dir, run_id, 'input.json'), 'wt') as f:
            json.dump(args, f)

        client_channel = ClientChannel(run_dir, run_id)
        p = subprocess.Popen([
            "spark-submit",
            os.path.join(deployment_location, "job_loader.py"),
            "--run-id", run_id,
            "--run-dir", run_dir,
            "--app-dir", app_dir
        ])
        exit_code = None
        cli_entered = False

        if on_job_submitted is not None:
            on_job_submitted(run_id, vendor_info={})

        while True:
            time.sleep(1)
            exit_code = p.poll()
            if exit_code is not None:
                break

            if cli_mode and not cli_entered:
                cli_entered = True
                cli_handler = CLIHandler(
                    client_channel,
                    lambda : p.poll() is None,
                    handlers
                )
                cli_handler.loop()

        if exit_code != 0:
            Exception(f"Job failed, exit_code = {exit_code}")

        print("Job completed successfully")
        with open(os.path.join(run_dir, run_id, "result.json"), "r") as f:
            return json.load(f)
