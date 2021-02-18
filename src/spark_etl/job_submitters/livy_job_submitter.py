import os
import json
import time
import subprocess
from urllib.parse import urlparse
import uuid
import tempfile

from requests.auth import HTTPBasicAuth
import requests

from .abstract_job_submitter import AbstractJobSubmitter
from spark_etl.utils import CLIHandler
from spark_etl.core import ClientChannelInterface
from spark_etl.exceptions import SparkETLLaunchFailure

class ClientChannel(ClientChannelInterface):
    def __init__(self, bridge, stage_dir, run_dir, run_id):
        self.bridge = bridge        # the bridge server's name which we can ssh to
        self.stage_dir = stage_dir  # stage_dir is on bridge
        self.run_dir = run_dir      # base dir for all runs, e.g. hdfs:///beta/etl/runs
        self.run_id  = run_id       # string, unique id of the run


    def _create_stage_dir(self):
        # We are using stage_dir instead of temp dir to stage objects on bridge
        # the reason is, many cases temp dir is in memory and cannot hold large object
        subprocess.check_call(
            [
                "ssh", "-q", self.bridge,
                "mkdir", "-p", os.path.join(self.stage_dir, self.run_id)
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )


    def read_json(self, name):
        """read a json object from HDFS run dir with specific name
        """
        self._create_stage_dir()

        # copy to stage dir on bridge first
        subprocess.check_call(
            [
                "ssh", "-q", self.bridge,
                "hdfs", "dfs", "-copyToLocal", "-f",
                os.path.join(self.run_dir, self.run_id, name),
                os.path.join(self.stage_dir, self.run_id, name)
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )

        try:
            stage_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
            stage_file.close()

            subprocess.check_call(
                [
                    "scp", "-q",
                    os.path.join(
                        f"{self.bridge}:{self.stage_dir}", self.run_id, name
                    ),
                    stage_file.name
                ],
                stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
            )

            with open(stage_file.name) as f:
                return json.load(f)
        finally:
            os.remove(stage_file.name)


    def has_json(self, name):
        """check if a file with specific name exist or not in the HDFS run dir
        """
        exit_code = subprocess.call(
            [
                "ssh", "-q", self.bridge,
                "hdfs", "dfs", "-test", "-f",
                os.path.join(self.run_dir, self.run_id, name)
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )
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
            subprocess.check_call(
                [
                    "scp", "-q",
                    stage_file.name,
                    os.path.join(
                        f"{self.bridge}:{self.stage_dir}", self.run_id, name
                    )
                ],
                stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
            )

            # then upload to HDFS, -f for overwrite if exist
            subprocess.check_call(
                [
                    "ssh", "-q", self.bridge,
                    "hdfs", "dfs", "-copyFromLocal", "-f",
                    os.path.join(self.stage_dir, self.run_id, name),
                    os.path.join(self.run_dir, self.run_id, name)
                ],
                stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
            )
        finally:
            os.remove(stage_file.name)


    def delete_json(self, name):
        subprocess.check_call(
            [
                "ssh", "-q", self.bridge,
                "hdfs", "dfs", "-rm",
                os.path.join(self.run_dir, self.run_id, name)
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
        )


class LivyJobSubmitter(AbstractJobSubmitter):
    def __init__(self, config):
        super(LivyJobSubmitter, self).__init__(config)

    # options is vendor specific arguments
    # args.conf is the spark job config
    # args is arguments need to pass to the main entry
    def run(self, deployment_location, options={}, args={}, handlers=[], on_job_submitted=None, cli_mode=False):
        bridge = self.config["bridge"]
        stage_dir = self.config['stage_dir']
        run_dir = self.config['run_dir']

        # create run dir
        run_id = str(uuid.uuid4())
        subprocess.check_call([
            "ssh", "-q", bridge,
            "hdfs", "dfs", "-mkdir", "-p",
            os.path.join(run_dir, run_id)
        ])

        client_channel = ClientChannel(bridge, stage_dir, run_dir, run_id)
        client_channel.write_json("input.json", args)

        o = urlparse(deployment_location)
        if o.scheme not in ('hdfs', 's3'):
            raise SparkETLLaunchFailure("deployment_location must be in hdfs or s3")

        headers = {
            "Content-Type": "application/json",
            "X-Requested-By": "root",
            'proxyUser': 'root'
        }

        config = {
            'file': os.path.join(deployment_location, "job_loader.py"),
            'pyFiles': [ os.path.join(deployment_location, "app.zip") ],
            'args': [
                '--run-id', run_id,
                '--run-dir', os.path.join(run_dir, run_id),
                '--lib-zip', os.path.join(deployment_location, "lib.zip")
            ]
        }
        base_lib_dir = self.config.get('base_lib_dir')
        if base_lib_dir:
            config['args'].extend(['--base-lib-dir', base_lib_dir])

        config.update(options)
        config.pop("display_name", None)  # livy job submitter does not support display_name

        service_url = self.config['service_url']
        username = self.config.get('username')
        password = self.config.get('password')

        # print(json.dumps(config))
        if username is None:
            r = requests.post(
                os.path.join(service_url, "batches"),
                data=json.dumps(config),
                headers=headers,
                verify=False
            )
        else:
            r = requests.post(
                os.path.join(service_url, "batches"),
                data=json.dumps(config),
                headers=headers,
                auth=HTTPBasicAuth(username, password),
                verify=False
            )
        if r.status_code not in [200, 201]:
            msg = "Failed to submit the job, status: {}, error message: \"{}\"".format(
                r.status_code,
                r.content
            )
            raise Exception(msg)

        print(f'job submitted, run_id = {run_id}')
        ret = json.loads(r.content.decode("utf8"))
        if on_job_submitted is not None:
            on_job_submitted(run_id, vendor_info=ret)
        job_id = ret['id']
        print('job id: {}'.format(job_id))
        print(ret)
        print('logs:')
        for log in ret.get('log', []):
            print(log)

        def get_job_status():
            r = requests.get(
                os.path.join(service_url, "batches", str(job_id)),
                headers=headers,
                auth=HTTPBasicAuth(username, password),
                verify=False
            )
            if r.status_code != 200:
                msg = "Failed to get job status, status: {}, error message: \"{}\"".format(
                    r.status_code,
                    r.content
                )
                raise Exception(msg)
            ret = json.loads(r.content.decode("utf8"))
            return ret

        cli_entered = False
        # pull the job status
        while True:
            ret = get_job_status()
            job_state = ret['state']
            appId = ret.get('appId')
            print('job_state: {}, applicationId: {}'.format(
                job_state, appId

            ))
            if job_state in ['success', 'dead', 'killed']:
                # cmd = f"yarn logs -applicationId {appId}"
                # host = self.config['bridge']
                # subprocess.call(["ssh", "-q", "-t", host, cmd], shell=False)
                print(f"job_state={job_state}")
                if job_state == 'success':
                    return client_channel.read_json("result.json")
                raise Exception("Job failed")
            time.sleep(5)

            # we enter the cli mode upon first reached the running status
            if cli_mode and not cli_entered and job_state == 'running':
                cli_entered = True
                cli_handler = CLIHandler(
                    client_channel,
                    lambda : get_job_status()['state'] not in ('success', 'dead', 'killed'),
                    handlers
                )
                cli_handler.loop()


