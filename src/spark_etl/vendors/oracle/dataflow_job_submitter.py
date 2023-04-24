import logging
logger = logging.getLogger(__name__)

from urllib.parse import urlparse
import time
import uuid
from datetime import datetime, timedelta
import os
from pathlib import Path
import json

import oci
from oci_core import get_os_client, get_df_client, os_upload_json, os_download_json, \
    os_has_object, os_delete_object

from spark_etl.job_submitters import AbstractJobSubmitter
from spark_etl import SparkETLLaunchFailure
from .tools import check_response, remote_execute
from spark_etl.utils import CLIHandler, handle_server_ask
from spark_etl.core import ClientChannelInterface

class ClientChannel(ClientChannelInterface):
    def __init__(self, region, oci_config, run_dir, run_id):
        self.region = region
        self.oci_config = oci_config
        self.run_dir = run_dir
        self.run_id = run_id

        o = urlparse(run_dir)
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
        # run_dir, uri, point to the run directory.
        run_dir = self.config.get('run_dir') or self.config.get('run_base_dir')
        if run_dir is None:
            raise Exception("run_dir is not specified")
        o = urlparse(run_dir)
        if o.scheme != 'oci':
            raise SparkETLLaunchFailure("run_dir must be in OCI")


    @property
    def region(self):
        return self.config['region']



    def run(self, deployment_location, options={}, args={}, handlers=None, on_job_submitted=None, cli_mode=False):
        # options fields
        #     num_executors     : number
        #     driver_shape      :  string
        #     executor_shape    : string
        #     lib_url_duration  : number (repre the number of minutes)
        #     on_job_submitted  : callback, on_job_submitted(run_id, vendor_info={'oci_run_id': 'xxxyyy'})

        o = urlparse(deployment_location)
        if o.scheme != 'oci':
            raise SparkETLLaunchFailure("deployment_location must be in OCI")

        # try to load run_id and oci_run_id from state file
        run_id = None
        oci_run_id = None
        state_filename = options.get("state_filename")
        if state_filename is not None and os.path.isfile(state_filename):
            with open(state_filename, "rt") as f:
                state = json.load(f)
                run_id = state.get("run_id")
                oci_run_id = state.get("oci_run_id")
        if run_id is None:            # we are launching a new job
            run_id = str(uuid.uuid4())
            logger.info(f"run_id generated: {run_id}")
            oci_run_id = None
        else:                         # we are resuming an existing job
            logger.info(f"run_id provided: {run_id}")
            oci_run_id = options.get("resume", {}).get("oci_run_id")
            logger.info(f"oci_run_id = {oci_run_id}")
            assert oci_run_id is not None

        run_dir = self.config.get('run_dir') or self.config.get('run_base_dir')

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
            run_dir,
            run_id
        )
        if oci_run_id is None:
            # only upload job args if we want to run job, in the resume case, do not upload
            client_channel.write_json("args.json", args)

        df_client = get_df_client(self.region, self.config.get("oci_config"))
        df_client_time = datetime.utcnow()
        crd_argv = {
            'compartment_id': deployment['compartment_id'],
            'application_id': deployment['application_id'],
            'display_name' :options["display_name"],
            'arguments': [
                "--deployment-location", deployment_location,
                "--run-id", run_id,
                "--run-dir", os.path.join(run_dir, run_id),
                "--app-region", self.region,
            ],
        }
        for key in ['num_executors', 'driver_shape', 'executor_shape', 'configuration']:
            if key in options:
                crd_argv[key] = options[key]

        if oci_run_id is None:
            create_run_details = oci.data_flow.models.CreateRunDetails(**crd_argv)
            r = df_client.create_run(create_run_details=create_run_details)
            check_response(r, lambda : SparkETLLaunchFailure("dataflow failed to run the application"))
            run = r.data
            oci_run_id = run.id
            logger.info(f"Job launched, run_id = {run_id}, oci_run_id = {run.id}")
            
            if state_filename is not None:
                os.path.makedirs(
                    str(Path(state_filename).parent.absolute()),
                    exist_ok=True
                )
                with open(state_filename, "wt") as f:
                    json.dump({"run_id": run_id, "oci_run_id": oci_run_id}, state_filename)

            if on_job_submitted is not None:
                on_job_submitted(run_id, vendor_info={'oci_run_id': run.id})
            

        cli_entered = False
        while True:
            if datetime.utcnow() - df_client_time >= timedelta(hours=1):
                df_client = get_df_client(self.region, self.config.get("oci_config"))
                df_client_time = datetime.utcnow()
            time.sleep(10)
            r = df_client.get_run(run_id=run.id)
            check_response(r, lambda : SparkETLGetStatusFailure("dataflow failed to get run status"))
            run = r.data
            logger.info(f"Status: {run.lifecycle_state}")
            if run.lifecycle_state in ('FAILED', 'SUCCEEDED', 'CANCELED'):
                if state_filename is not None and os.path.isfile(state_filename):
                    os.remove(state_filename)
                break
            handle_server_ask(client_channel, handlers)

            if cli_mode and not cli_entered and run.lifecycle_state == 'IN_PROGRESS':
                cli_entered = True
                cli_handler = CLIHandler(client_channel, None, handlers)
                cli_handler.loop()


        if run.lifecycle_state in ('FAILED', 'CANCELED'):
            raise Exception(f"Job failed with status: {run.lifecycle_state}")
        return client_channel.read_json('result.json')
        # return {
        #     'state': run.lifecycle_state,
        #     'run_id': run_id,
        #     'succeeded': run.lifecycle_state == 'SUCCEEDED'
        # }



    # def kill(self, run_id):
    #     df_client = get_df_client(self.region)

    #     r = df_client.delete_run(run_id)
    #     check_response(r, lambda : SparkETLKillFailure("dataflow failed to kill the run"))
