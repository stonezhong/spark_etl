import json
from urllib.parse import urlparse
import time
import uuid
from datetime import datetime, timedelta

import oci
from oci_core import get_os_client, get_df_client, os_upload, os_upload_json, os_download, os_download_json, os_get_endpoint

from spark_etl.job_submitters import AbstractJobSubmitter
from spark_etl import SparkETLLaunchFailure, SparkETLGetStatusFailure, SparkETLKillFailure
from .tools import check_response, remote_execute

class DataflowJobSubmitter(AbstractJobSubmitter):
    def __init__(self, config):
        super(DataflowJobSubmitter, self).__init__(config)
        # config fields
        # region, e.g. IAD
        # run_base_dir, uri, point to the run directory.
        run_base_dir = self.config['run_base_dir']
        o = urlparse(run_base_dir)
        if o.scheme != 'oci':
            raise SparkETLLaunchFailure("run_base_dir must be in OCI")


    @property
    def region(self):
        return self.config['region']

    def run(self, deployment_location, options={}, args={}, handlers=[]):
        # options fields
        #     num_executors     : number
        #     driver_shape      :  string
        #     executor_shape    : string
        #     lib_url_duration  : number (repre the number of minutes)
        o = urlparse(deployment_location)
        if o.scheme != 'oci':
            raise SparkETLLaunchFailure("deployment_location must be in OCI")

        run_base_dir = self.config['run_base_dir']
        run_id = str(uuid.uuid4())

        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        root_path = o.path[1:] # remove the leading "/"

        # let's get the deployment.json
        os_client = get_os_client(self.region, self.config.get("oci_config"))
        deployment = os_download_json(os_client, namespace, bucket, f"{root_path}/deployment.json")

        lib_url_duration = options.get("lib_url_duration", 30)
        r = os_client.create_preauthenticated_request(
            namespace,
            bucket,
            oci.object_storage.models.CreatePreauthenticatedRequestDetails(
                access_type = 'ObjectRead',
                name=f'for run {run_id}',
                object_name=f'{root_path}/lib.zip',
                time_expires=datetime.utcnow() + timedelta(minutes=lib_url_duration)
            )
        )
        check_response(r, lambda : SparkETLLaunchFailure("dataflow failed to get lib url"))
        lib_url = f"{os_get_endpoint(self.region)}{r.data.access_uri}"

        # let's upload the args
        o = urlparse(self.config['run_base_dir'])
        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        root_path = o.path[1:] # remove the leading "/"
        os_upload_json(os_client, args, namespace, bucket, f"{root_path}/{run_id}/args.json")

        df_client = get_df_client(self.region, self.config.get("oci_config"))
        crd_argv = {
            'compartment_id': deployment['compartment_id'],
            'application_id': deployment['application_id'],
            'display_name' :options["display_name"],
            'arguments': [
                "--deployment-location", deployment_location,
                "--run-id", run_id,
                "--run-dir", f"{run_base_dir}/{run_id}",
                "--app-region", self.region,
                "--lib-url", lib_url,
            ],
        }
        for key in ['num_executors', 'driver_shape', 'executor_shape']:
            if key in options:
                crd_argv[key] = options[key]

        create_run_details = oci.data_flow.models.CreateRunDetails(**crd_argv)
        r = df_client.create_run(create_run_details=create_run_details)
        check_response(r, lambda : SparkETLLaunchFailure("dataflow failed to run the application"))
        run = r.data
        oci_run_id = run.id
        print(f"Job launched, run_id = {run_id}, oci_run_id = {run.id}")

        while True:
            time.sleep(10)
            r = df_client.get_run(run_id=run.id)
            check_response(r, lambda : SparkETLGetStatusFailure("dataflow failed to get run status"))
            run = r.data
            print(f"Status: {run.lifecycle_state}")
            if run.lifecycle_state in ('FAILED', 'SUCCEEDED', 'CANCELED'):
                break
            self.handle_ask(run_id, handlers)

        if run.lifecycle_state in ('FAILED', 'CANCELED'):
            raise Exception(f"Job failed with status: {run.lifecycle_state}")
        return self.get_result(run_id)
        # return {
        #     'state': run.lifecycle_state,
        #     'run_id': run_id,
        #     'succeeded': run.lifecycle_state == 'SUCCEEDED'
        # }

    # job can send request to launcher
    def handle_ask(self, run_id, handlers):
        run_base_dir = self.config['run_base_dir']
        o = urlparse(run_base_dir)
        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        base_dir = o.path[1:]    # remove the leading "/"

        os_client = get_os_client(self.region, self.config.get("oci_config"))
        while True:
            r = os_client.list_objects(
                namespace, 
                bucket,
                prefix = f"{base_dir}/{run_id}/to_submitter/ask_",
                limit = 1
            )
            if len(r.data.objects) == 0:
                break
            object_name = r.data.objects[0].name
            content = os_download_json(os_client, namespace, bucket, object_name)
            print(f"Got ask: {content}")
            out = None
            handled = False
            for handler in handlers:
                handled, out = handler(content)
                if handled:
                    break
            if not handled:
                raise Exception("ask is not handled")
            
            print(f"Answer is: {out}")
            print("")
        
            ask_name = object_name.split("/")[-1]
            answer_name = "answer" + ask_name[3:]

            os_client.delete_object(namespace, bucket, object_name)
            os_upload_json(os_client, out, namespace, bucket, f"{base_dir}/{run_id}/to_submitter/{answer_name}")

    def get_result(self, run_id):
        result_object_name = f"{self.config['run_base_dir']}/{run_id}/result.json"
        o = urlparse(result_object_name)

        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        object_name = o.path[1:]    # remove the leading "/"

        os_client = get_os_client(self.region, self.config.get("oci_config"))
        result = os_download_json(os_client, namespace, bucket, object_name)
        return result


    def kill(self, run_id):
        df_client = get_df_client(self.region)

        r = df_client.delete_run(run_id)
        check_response(r, lambda : SparkETLKillFailure("dataflow failed to kill the run"))
