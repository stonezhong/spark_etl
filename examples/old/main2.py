#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from pywebhdfs.webhdfs import PyWebHdfsClient
import requests
import json
import time
import os

class HDFS(object):
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username

        request_extra_opts = {
            'auth': (username, password, )
        }
        
        self.hdfs = PyWebHdfsClient(host=host, 
                                    port=port, 
                                    user_name=username,
                                    request_extra_opts=request_extra_opts)
    
    def upload(self, local_base_dir, remote_base_dir, local_filename):
        # local_filename is relative to local_base_dir
        target_filename = os.path.join(remote_base_dir, local_filename)
        self.hdfs.make_dir(os.path.dirname(target_filename))

        with open(os.path.join(local_base_dir, local_filename), 'rb') as f:
            # TODO: deal with large file
            data = f.read()
        
        self.hdfs.create_file(
            os.path.join(remote_base_dir, local_filename), data
        )
    
    def read_text_file(self, filename):
        return self.hdfs.read_file(filename).decode('utf-8')

    def read_json_file(self, filename):
        return json.loads(self.read_text_file(filename))

class DeploymentManager(object):
    # DeploymentManager is responsible to deploy pyspark app to HDFS
    def __init__(self, hdfs, hdfs_app_path):
        self.hdfs = hdfs
        self.hdfs_app_path = hdfs_app_path
    
    def deploy(self, local_app_path):
        with open(os.path.join(local_app_path, 'manifest.json')) as f:
            manifest = json.load(f)

        app_version = manifest['version']
        app_name = manifest['name']
        entry = manifest['entry']
        
        hdfs_app_home = os.path.join(self.hdfs_app_path, app_name, app_version)
        self.hdfs.upload(
            local_app_path,
            hdfs_app_home,
            'manifest.json'
        )

        self.hdfs.upload(
            local_app_path,
            hdfs_app_home,
            entry
        )
    
class JobSubmitter(object):
    # JobSubmitter is responsible to submit spark job to spark cluster
    def __init__(self, hdfs, livy_base_url, hdfs_app_path):
        self.hdfs = hdfs
        self.hdfs_app_path = hdfs_app_path
        self.livy_base_url = livy_base_url

    def submit(self, app_name, app_version):
        hdfs_app_home = os.path.join(self.hdfs_app_path, app_name, app_version)
        manifest = self.hdfs.read_json_file(os.path.join(hdfs_app_home, 'manifest.json'))

        data = {
            'file': f"hdfs://{hdfs_app_home}/{manifest['entry']}",
        }

        if manifest.get('use_python3'):
            data.update({
                'conf': {
                    'spark.yarn.appMasterEnv.PYSPARK_PYTHON': 'python3',
                    'spark.executorEnv.PYSPARK_PYTHON': 'python3'
                }
            })

        r = requests.post(url=f"{self.livy_base_url}/batches", 
                          json=data)
        r.raise_for_status()

        out = r.json()
        job_id = out['id']
        
        # Now let's wait for the job to finish
        while True:
            r = requests.get(f'{self.livy_base_url}/batches/{job_id}')
            r.raise_for_status()
            status = r.json()
            print("")
            print(status)
            print("")
            state = status['state']
            if state == 'success':
                break
            if state == 'dead':
                raise Exception('job failed')
            time.sleep(20)


def main():
    hdfs = HDFS(host='10.0.0.11', 
                port=60007, 
                username='root', 
                password='changeme')

    dm = DeploymentManager(hdfs=hdfs, hdfs_app_path='/etl_home/apps')
    dm.deploy('/home/stonezhong/DATA_DISK/projects/spark_etl/examples/myjob')

    job_submitter = JobSubmitter(hdfs, 'http://10.0.0.11:60008', hdfs_app_path='/etl_home/apps')
    job_submitter.submit('sparktest', '0.0.0.1')

    return

if __name__ == '__main__':
    main()