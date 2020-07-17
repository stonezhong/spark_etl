import json
import time
import subprocess
from urllib.parse import urlparse

from requests.auth import HTTPBasicAuth
import requests

from .abstract_job_submitter import AbstractJobSubmitter

class LivyJobSubmitter(AbstractJobSubmitter):
    def __init__(self, config):
        super(LivyJobSubmitter, self).__init__(config)

    def run(self, deployment_location, options={}, args={}):
        o = urlparse(deployment_location)
        if o.scheme != 'hdfs':
            raise SparkETLDeploymentFailure("deployment_location must be in hdfs")

        headers = {
            "Content-Type": "application/json",
            "X-Requested-By": "root",
            'proxyUser': 'root'
        }

        print(deployment_location)

        config = {
            'file': f'{deployment_location}/main.py',
            'pyFiles': [f'{deployment_location}/app.zip'],
        }

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
                cmd = f"yarn logs -applicationId {appId}"
                host = self.config['bridge']
                subprocess.call(["ssh", "-q", "-t", host, cmd], shell=False)
                return job_state
            time.sleep(5)
