import os
import json
import time
import subprocess
from urllib.parse import urlparse
import uuid
import tempfile
from copy import deepcopy

from requests.auth import HTTPBasicAuth
import requests

from .abstract_job_submitter import AbstractJobSubmitter
from spark_etl.utils import CLIHandler
from spark_etl.core import ClientChannelInterface
from spark_etl.exceptions import SparkETLLaunchFailure
from spark_etl.ssh_config import SSHConfig

from spark_etl.core.hdfs_client import HDFSClientChannel

#################################################################################
# For ssh, we allow user to specify their own configs
# The config file is normal ssh config file, however, the IdentityFile should
# point to a key file id instead of filename
#
# The entire config is a json, which:
# {
#     config: string, same as ssh config,
#     keys: {
#         "foo": "...",         # content for key "foo"
#         "bar": "..."          # content for key "bar"
#     }
# }
#################################################################################


class LivyJobSubmitter(AbstractJobSubmitter):
    def __init__(self, config):
        super(LivyJobSubmitter, self).__init__(config)
        self.ssh_config = SSHConfig(config['ssh_config'])

    # options is vendor specific arguments
    # args.conf is the spark job config
    # args is arguments need to pass to the main entry
    def run(self, deployment_location, options={}, args={}, handlers=[], on_job_submitted=None, cli_mode=False):
        run_id = str(uuid.uuid4())
        run_dir = self.config['run_dir']
        # run dir has to be a URI since it shoud be reachable from both
        # client and spark cluster
        run_home_dir = os.path.join(run_dir, run_id)

        bridge = self.config["bridge"]
        stage_dir = self.config['stage_dir']
        spark_host_stage_dir = self.config['spark_host_stage_dir']
        self.ssh_config.generate()
        tunnel = None
        local_livy_port = None

        # by default, livy server is the same as the bridge
        # and ssh tunnel is used for livy access
        livy_cfg = self.config.get('livy', {
            'host': '127.0.0.1',
            'port': 8998,
            'via_tunnel': True,
            'protocol': 'http',
            'username': None,
            'password': None,
        })
        livy_cfg_host = livy_cfg.get('host', '127.0.0.1')
        livy_cfg_port = livy_cfg.get('port', 8998)
        livy_cfg_via_tunnel = livy_cfg.get('via_tunnel', True)
        livy_cfg_protocol = livy_cfg.get('protocol', 'http')
        livy_cfg_username = livy_cfg.get('username', None)
        livy_cfg_password = livy_cfg.get('password', None)
        try:
            if livy_cfg_via_tunnel:
                local_livy_port, tunnel = self.ssh_config.tunnel(bridge, livy_cfg_host, livy_cfg_port)
            o = urlparse(deployment_location)
            if o.scheme not in ('hdfs', 's3', 's3a'):
                raise SparkETLLaunchFailure("deployment_location must be in hdfs, s3 or s3a")
            
            client_channel = HDFSClientChannel(
                bridge, stage_dir, run_dir, run_id,
                ssh_config = self.ssh_config
            )
            # create run home dir
            self.ssh_config.execute(bridge, [
                "hdfs", "dfs", "-mkdir", "-p", run_home_dir
            ])

            client_channel.write_json("input.json", args)
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
                    '--lib-zip', os.path.join(deployment_location, "lib.zip"),
                    '--spark-host-stage-dir', spark_host_stage_dir,
                ]
            }

            config.update(options)
            config.pop("display_name", None)  # livy job submitter does not support display_name

            if livy_cfg_via_tunnel:
                livy_service_url = f"{livy_cfg_protocol}://127.0.0.1:{local_livy_port}/"
            else:
                livy_service_url = f"{livy_cfg_protocol}://{livy_cfg_host}:{livy_cfg_port}/"

            print(f"Livy: {livy_service_url}")
            # print(json.dumps(config, indent=4, separators=(',', ': ')))
            if livy_cfg_username is None:
                r = requests.post(
                    os.path.join(livy_service_url, "batches"),
                    data=json.dumps(config),
                    headers=headers,
                    verify=False
                )
            else:
                r = requests.post(
                    os.path.join(livy_service_url, "batches"),
                    data=json.dumps(config),
                    headers=headers,
                    auth=HTTPBasicAuth(livy_cfg_username, livy_cfg_password),
                    verify=False
                )
            if r.status_code not in [200, 201]:
                msg = "Failed to submit the job, status: {}, error message: \"{}\"".format(
                    r.status_code,
                    r.content
                )
                raise SparkETLLaunchFailure(msg)

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
                if livy_cfg_username is None:
                    r = requests.get(
                        os.path.join(livy_service_url, "batches", str(job_id)),
                        headers=headers,
                        verify=False
                    )
                else:
                    r = requests.get(
                        os.path.join(livy_service_url, "batches", str(job_id)),
                        headers=headers,
                        auth=HTTPBasicAuth(livy_cfg_username, livy_cfg_password),
                        verify=False
                    )
                if r.status_code != 200:
                    msg = "Failed to get job status, status: {}, error message: \"{}\"".format(
                        r.status_code,
                        r.content
                    )
                    # this is considered unhandled
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
                    raise SparkETLLaunchFailure("Job failed")
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
        finally:
            if tunnel is not None:
                tunnel.kill()
            self.ssh_config.destroy()


