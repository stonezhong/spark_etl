import json
import os
import uuid
import subprocess
import json
import time
from urllib.parse import urlparse

from spark_etl.job_submitters import AbstractJobSubmitter
from spark_etl.utils import CLIHandler

class PySparkJobSubmitter(AbstractJobSubmitter):
    def __init__(self, config):
        super(PySparkJobSubmitter, self).__init__(config)
        ####################################################################################
        # here is a sample config
        # {
        #     "run_dir": "/home/stonezhong/spark-etl-lab/src/local-lake/runs",
        #     "enable_aws_s3": true,
        #     "aws_account": "~/.aws/account.json",
        # }
        # enable_aws_s3: set to True if you want to access AWS S3 buckets
        # aws_account: point to a json filename, the json file looks like below
        # {
        #     "aws_access_key_id": "***",
        #     "aws_secret_access_key": "***"
        # }
        # If aws_account is missing, you need to provide both "aws_access_key_id" and "aws_secret_access_key"
        ####################################################################################

    def run(self, deployment_location, options={}, args={}, handlers=[], on_job_submitted=None, cli_mode=False):
        # version is already baked into deployment_location
        # TODO: deal with handlers
        run_id  = str(uuid.uuid4())
        run_dir = self.config['run_dir']
        if run_dir.startswith("file://"):
            run_dir = run_dir[7:]
        run_home_dir = os.path.join(run_dir, run_id)

        enable_aws_s3 = self.config.get('enable_aws_s3', False)
        if enable_aws_s3:
            aws_account = self.config.get("aws_account")
            if aws_account is None:
                # set pass
                aws_access_key_id = self.config["aws_access_key_id"]
                aws_secret_access_key = self.config["aws_secret_access_key"]
            else:
                aws_account = os.path.expandvars(os.path.expanduser(aws_account))
                with open(aws_account, "rt") as f:
                    aws_account_content = json.load(f)
                    aws_access_key_id = aws_account_content['aws_access_key_id']
                    aws_secret_access_key = aws_account_content['aws_secret_access_key']

        o = urlparse(run_dir)
        if o.scheme in ("s3", "s3a"):
            if not enable_aws_s3:
                raise Exception("You need to set enable_aws_s3 to True since your run_dir in using s3")
            from spark_etl.core.s3_client import S3ClientChannel
            client_channel = S3ClientChannel(run_home_dir, aws_access_key_id, aws_secret_access_key)
        elif o.scheme == "":
            from spark_etl.core.local_client import LFSClientChannel
            os.makedirs(run_home_dir)
            client_channel = LFSClientChannel(run_home_dir)
        else:
            raise Exception(f"run_dir can only be in s3, s3a, file or a local file path")

        # generate input.json
        client_channel.write_json("input.json", args)

        run_args = [
            "spark-submit",
        ]
        run_args.extend([
            "--py-files",
            os.path.join(deployment_location, "app.zip")
        ])

        spark_host_stage_dir = os.path.join(os.environ["HOME"], ".pyspark")
        s3_buffer_dir  = os.path.join(os.environ["HOME"], ".s3-buffer", run_id)

        if enable_aws_s3:
            run_args.extend([
                "-c",
                'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem'
            ])
            run_args.extend([
                "-c",
                f'spark.hadoop.fs.s3a.buffer.dir={s3_buffer_dir}'
            ])
            run_args.extend([
                "-c",
                f'spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
            ])
            run_args.extend([
                "-c",
                f'spark.hadoop.fs.s3a.access.key={aws_access_key_id}'
            ])
            run_args.extend([
                "-c",
                f'spark.hadoop.fs.s3a.secret.key={aws_secret_access_key}'
            ])

        run_args.extend([
            os.path.join(deployment_location, "job_loader.py"),
            "--run-id", run_id,
            "--run-dir", run_home_dir,
            '--lib-zip', os.path.join(deployment_location, "lib.zip"),
            "--spark-host-stage-dir", spark_host_stage_dir
        ])
        if cli_mode:
            run_args.append("--cli-mode")

        myenv = os.environ.copy()
        myenv['SPARK_LOCAL_HOSTNAME'] = 'localhost'
        p = subprocess.Popen(run_args, env=myenv)
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
        result = client_channel.read_json("result.json")
        return result
