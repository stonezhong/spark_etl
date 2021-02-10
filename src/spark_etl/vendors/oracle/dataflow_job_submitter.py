import json
from urllib.parse import urlparse
import time
import uuid
from datetime import datetime, timedelta
import os
from termcolor import colored, cprint
import readline

import oci
from oci_core import get_os_client, get_df_client, os_upload, os_upload_json, os_download, os_download_json, os_get_endpoint, \
    os_has_object, os_delete_object

from spark_etl.job_submitters import AbstractJobSubmitter
from spark_etl import SparkETLLaunchFailure, SparkETLGetStatusFailure, SparkETLKillFailure
from .tools import check_response, remote_execute
from spark_etl.utils import CLIHandler
from spark_etl.core import ClientChannelInterface

class ClientChannel(ClientChannelInterface):
    def __init__(self, region, oci_config, run_base_dir, run_id):
        self.region = region
        self.oci_config = oci_config
        self.run_base_dir = run_base_dir
        self.run_id = run_id

        o = urlparse(run_base_dir)
        self.namespace = o.netloc.split('@')[1]
        self.bucket = o.netloc.split('@')[0]
        self.root_path = o.path[1:] # remove the leading "/"


    def read_json(self, name):
        os_client = get_os_client(self.region, self.oci_config)
        object_name = os.path.join(self.root_path, self.run_id, name)
        result = os_download_json(os_client, self.namespace, self.bucket, object_name)
        return result


    def write_json(self, name, payload):
        os_client = get_os_client(self.region, self.oci_config)
        object_name = os.path.join(self.root_path, self.run_id, name)
        os_upload_json(os_client, payload, self.namespace, self.bucket, object_name)


    def has_json(self, name):
        os_client = get_os_client(self.region, self.oci_config)
        object_name = os.path.join(self.root_path, self.run_id, name)
        return os_has_object(os_client, self.namespace, self.bucket, object_name)


    def delete_json(self, name):
        os_client = get_os_client(self.region, self.oci_config)
        object_name = os.path.join(self.root_path, self.run_id, name)
        os_delete_object(os_client, self.namespace, self.bucket, object_name)


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



    def run(self, deployment_location, options={}, args={}, handlers=[], on_job_submitted=None, cli_mode=False):
        # options fields
        #     num_executors     : number
        #     driver_shape      :  string
        #     executor_shape    : string
        #     lib_url_duration  : number (repre the number of minutes)
        #     on_job_submitted  : callback, on_job_submitted(run_id, vendor_info={'oci_run_id': 'xxxyyy'})

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
        deployment = os_download_json(os_client, namespace, bucket, os.path.join(root_path, "deployment.json"))

        # let's upload the args
        client_channel = ClientChannel(
            self.region,
            self.config.get("oci_config"),
            run_base_dir,
            run_id
        )
        client_channel.write_json("args.json", args)

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
                "--run-dir", os.path.join(run_base_dir, run_id),
                "--app-region", self.region,
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
        if on_job_submitted is not None:
            on_job_submitted(run_id, vendor_info={'oci_run_id': run.id})

        cli_entered = False
        while True:
            time.sleep(10)
            r = df_client.get_run(run_id=run.id)
            check_response(r, lambda : SparkETLGetStatusFailure("dataflow failed to get run status"))
            run = r.data
            print(f"Status: {run.lifecycle_state}")
            if run.lifecycle_state in ('FAILED', 'SUCCEEDED', 'CANCELED'):
                break
            self.handle_ask(run_id, handlers)

            if cli_mode and not cli_entered and run.lifecycle_state == 'IN_PROGRESS':
                cli_entered = True
                cli_handler = CLIHandler(client_channel, None)
                cli_handler.loop()


        if run.lifecycle_state in ('FAILED', 'CANCELED'):
            raise Exception(f"Job failed with status: {run.lifecycle_state}")
        return client_channel.read_json('result.json')
        # return {
        #     'state': run.lifecycle_state,
        #     'run_id': run_id,
        #     'succeeded': run.lifecycle_state == 'SUCCEEDED'
        # }

    def _list_objects(self, os_client, namespace, bucket, prefix, limit, retry_count = 6):
        if not isinstance(retry_count, int):
            raise ValueError(f"retry_count MUST be int")
        if retry_count < 1:
            raise ValueError(f"retry_count = {retry_count}")
        for i in range(0, retry_count):
            try:
                r = os_client.list_objects(
                    namespace,
                    bucket,
                    prefix = prefix,
                    limit = limit
                )
                return r
            except oci.exceptions.ServiceError as e:
                if e.status == 503:
                    print("oci os_client.list_object failed with 503, retrying ...")
                    time.sleep(10)
                    continue
        raise Exception(f"OCI list_object failed after {retry_count} retries")

    # job can send request to launcher
    def handle_ask(self, run_id, handlers):
        if len(handlers) == 0:
            return
        run_base_dir = self.config['run_base_dir']
        o = urlparse(run_base_dir)
        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        base_dir = o.path[1:]    # remove the leading "/"

        os_client = get_os_client(self.region, self.config.get("oci_config"))
        while True:
            r = self._list_objects(
                os_client,
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
            exception_name = None
            exception_msg = None
            # TODO: shall we ask handler to provide a name so we can track down
            #       which handler blows up?
            try:
                for handler in handlers:
                    handled, out = handler(content)
                    if handled:
                        break
                # if not handled:
                #     raise Exception("ask is not handled")
            except Exception as e:
                exception_name = e.__class__.__name__
                exception_msg = str(e)

            if not handled:
                if exception_name:
                    answer = {
                        "status": "exception",
                        "exception_name": exception_name,
                        "exception_msg": exception_msg
                    }
                    print(f"Exception {exception_name} happened during handling the question, error message: {exception_msg}")
                else:
                    answer = {
                        "status": "unhandled"
                    }
                    print(f"Ask is not handled, probably missing required handler!")
            else:
                print(f"Ask is handled, answer is: {out}")
                answer = {
                    "status": "ok",
                    "reply": out
                }

            ask_name = object_name.split("/")[-1]
            answer_name = "answer" + ask_name[3:]

            os_client.delete_object(namespace, bucket, object_name)
            os_upload_json(os_client, answer, namespace, bucket, f"{base_dir}/{run_id}/to_submitter/{answer_name}")



    def kill(self, run_id):
        df_client = get_df_client(self.region)

        r = df_client.delete_run(run_id)
        check_response(r, lambda : SparkETLKillFailure("dataflow failed to kill the run"))
