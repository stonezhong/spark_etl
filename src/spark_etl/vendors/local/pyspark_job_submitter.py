import json
import os
import uuid
import tempfile
import subprocess

from spark_etl.job_submitters import AbstractJobSubmitter

class PySparkJobSubmitter(AbstractJobSubmitter):
    def __init__(self, config):
        super(PySparkJobSubmitter, self).__init__(config)


    def run(self, deployment_location, options={}, args={}):
        # version is already baked into deployment_location
        run_id  = str(uuid.uuid4())
        run_dir = self.config['run_dir']
        app_dir = deployment_location

        os.makedirs(os.path.join(run_dir, run_id))

        # generate input.json
        with open(os.path.join(run_dir, run_id, 'input.json'), 'wt') as f:
            json.dump(args, f)

        subprocess.check_call(["spark-submit", os.path.join(deployment_location, "job_loader.py"), "--run-id", run_id, "--run-dir", run_dir, "--app-dir", app_dir])
        with open(os.path.join(run_dir, run_id, "result.json"), "r") as f:
            return json.load(f)
