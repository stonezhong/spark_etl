from requests.auth import HTTPBasicAuth
import requests
import json
import time

def _is_status_success(status_code):
    return status_code >= 200 and status_code < 300

class Livy(object):
    def __init__(self, service_url, 
                 username=None, password=None, hdfs_username=None, 
                 auth_cert=None, auth_key=None, ca_cert=None, 
                 retry_count=5, retry_sleep=5
    ):
        self.service_url = service_url
        self.username = username
        self.password = password
        self.hdfs_username = hdfs_username
        self.retry_count = retry_count
        self.retry_sleep = retry_sleep
        self.auth_cert = auth_cert
        self.auth_key = auth_key
        self.ca_cert = ca_cert


    def submit_job(self, **kwargs):
        headers = {
            "Content-Type": "application/json",
            "X-Requested-By": "admin"
        }
        cmd = dict(kwargs)
        if "proxyUser" not in cmd:
            cmd["proxyUser"] = self.hdfs_username
        if self.auth_cert:
            r = requests.post(
                "{}/batches".format(self.service_url),
                data=json.dumps(cmd),
                headers=headers,
                cert=(self.auth_cert, self.auth_key, ),
                verify=self.ca_cert
            )
        else:
            r = requests.post(
                "{}/batches".format(self.service_url),
                data=json.dumps(cmd),
                headers=headers,
                auth=HTTPBasicAuth(self.username, self.password),
                verify=False
            )
        if not _is_status_success(r.status_code):
            raise Exception("submit_job: failed, status_code = {}".format(r.status_code))

        return json.loads(r.content.decode("utf8"))

    def _http_get(self, *argc, **kwargs):
        args = dict(kwargs)
        if self.auth_cert is not None:
            args['cert'] = (self.auth_cert, self.auth_key, )
            args['verify'] = self.ca_cert
        else:
            args['auth'] = HTTPBasicAuth(self.username, self.password)
            args['verify'] = False

        for i in range(0, self.retry_count):
            r = requests.get(*argc, **args)
            if _is_status_success(r.status_code):
                return r
            print("send_request: failed, status_code = {}".format(r.status_code))
            if i == self.retry_count - 1:
                raise Exception("send_request: maxium retry encountered")
            else:
                print("send_request: retrying")
                time.sleep(self.retry_sleep)
                continue

    def get_job(self, job_id):
        r = self._http_get(
            "{}/batches/{}".format(self.service_url, job_id),
        )
        return json.loads(r.content.decode("utf8"))

    def get_yarn_app_id(self, job_id):
        return self.get_job(job_id)["appId"]
