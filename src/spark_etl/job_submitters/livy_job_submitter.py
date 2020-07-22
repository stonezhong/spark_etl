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

def _execute(host, cmd, error_ok=False):
    r = subprocess.call(["ssh", "-q", "-t", host, cmd], shell=False)
    if not error_ok and r != 0:
        raise Exception(f"command {cmd} failed with exit code {r}")

class LivyJobSubmitter(AbstractJobSubmitter):
    def __init__(self, config):
        super(LivyJobSubmitter, self).__init__(config)

    # options is vendor specific arguments
    # args.conf is the spark job config
    # args is arguments need to pass to the main entry
    def run(self, deployment_location, options={}, args={}):
        bridge = self.config["bridge"]
        stage_dir = self.config['stage_dir']
        run_dir = self.config['run_dir']

        # create run dir
        run_id = str(uuid.uuid4())
        _execute(bridge, f"mkdir -p {stage_dir}/{run_id}")
        _execute(bridge, f"hdfs dfs -mkdir -p {run_dir}/{run_id}")

        # generate input.json
        tf = tempfile.NamedTemporaryFile(mode="wt", delete=False)
        json.dump(args, tf)
        tf.close()
        subprocess.check_call([
            'scp', '-q', tf.name, f"{bridge}:{stage_dir}/{run_id}/input.json"
        ])
        _execute(bridge, f"hdfs dfs -copyFromLocal {stage_dir}/{run_id}/input.json {run_dir}/{run_id}/input.json")
        os.remove(tf.name)

        o = urlparse(deployment_location)
        if o.scheme != 'hdfs':
            raise SparkETLDeploymentFailure("deployment_location must be in hdfs")

        headers = {
            "Content-Type": "application/json",
            "X-Requested-By": "root",
            'proxyUser': 'root'
        }

        config = {
            'file': f'{deployment_location}/job_loader.py',
            'pyFiles': [f'{deployment_location}/app.zip'],
            'args': [
                '--run-id', run_id, 
                '--run-dir', f"{run_dir}/{run_id}",
                '--lib-url', f'{deployment_location}/lib.zip'
            ]
        }
        config.update(options)

        service_url = self.config['service_url']
        if service_url.endswith("/"):
            service_url = service_url[:-1]

        username = self.config['username']
        password = self.config['password']

        r = requests.post(
            "{}/batches".format(service_url),
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

        print('job submitted')
        ret = json.loads(r.content.decode("utf8"))
        job_id = ret['id']
        print('job id: {}'.format(job_id))
        print(ret)
        print('logs:')
        for log in ret.get('log', []):
            print(log)
        
        # pull the job status
        while True:
            r = requests.get(
                "{}/batches/{}".format(service_url, job_id),
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
            job_state = ret['state']
            appId = ret.get('appId')
            print('job_state: {}, applicationId: {}'.format(
                job_state, appId

            ))
            if job_state in ['success', 'dead']:
                # cmd = f"yarn logs -applicationId {appId}"
                # host = self.config['bridge']
                # subprocess.call(["ssh", "-q", "-t", host, cmd], shell=False)
                print(f"job_state={job_state}")
                if job_state == 'success':
                    # get the result.json
                    _execute(bridge, f"hdfs dfs -copyToLocal {run_dir}/{run_id}/result.json {stage_dir}/{run_id}/result.json")
                    rf = tempfile.NamedTemporaryFile(mode="w", delete=False)
                    rf.close()
                    subprocess.check_call([
                        'scp', '-q', f"{bridge}:{stage_dir}/{run_id}/result.json", rf.name
                    ])

                    with open(rf.name, "r") as f:
                        return json.load(f)
                raise Exception("Job failed")
            time.sleep(5)
