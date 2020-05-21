from requests.auth import HTTPBasicAuth
import requests
import json
import time

class LivyJobSubmitter(object):
    def __init__(self, service_url, username, password):
        self.service_url = service_url
        self.username = username
        self.password = password

    # when job submit successfully, it return 'success'
    # if the job failed, it return 'dead'
    def submit_job(self):
        headers = {
            "Content-Type": "application/json",
            "X-Requested-By": "root",
            'proxyUser': 'root'
        }

        config = {
            'file': 'hdfs:///jobs/spark_test.py',
            'conf': {
                "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "python3",
                "spark.executorEnv.PYSPARK_PYTHON": "python3"
            }
            # 'name': 'foo'
        }

        r = requests.post(
            "{}/batches".format(self.service_url),
            data=json.dumps(config),
            headers=headers,
            auth=HTTPBasicAuth(self.username, self.password),
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
                "{}/batches/{}".format(self.service_url, job_id),
                headers=headers,
                auth=HTTPBasicAuth(self.username, self.password),
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
                
                return job_state
            time.sleep(5)
