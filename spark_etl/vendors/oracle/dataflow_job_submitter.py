import json
from urllib.parse import urlparse
import time

import oci

from spark_etl.job_submitters import AbstractJobSubmitter
from spark_etl import SparkETLLaunchFailure, SparkETLGetStatusFailure
from .tools import check_response, remote_execute, dump_json, get_os_client, get_df_client, os_upload, os_download_json

class DataflowJobSubmitter(AbstractJobSubmitter):
    def __init__(self, config):
        super(DataflowJobSubmitter, self).__init__(config)
        # config fields
        # region, e.g. IAD
    
    @property
    def region(self):
        return self.config['region']

    def run(self, deployment_location, options):
        o = urlparse(deployment_location)
        if o.scheme != 'oci':
            raise SparkETLLaunchFailure("destination_location must be in OCI")

        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        root_path = o.path[1:] # remove the leading "/"

        # let's get the deployment.json
        os_client = get_os_client(self.region)
        deployment = os_download_json(os_client, namespace, bucket, f"{root_path}/deployment.json")

        df_client = get_df_client(self.region)
        create_run_details = oci.data_flow.models.CreateRunDetails(
            compartment_id=deployment['compartment_id'],
            application_id=deployment['application_id'],
            display_name=options["display_name"],
        )
        r = df_client.create_run(create_run_details=create_run_details)
        check_response(r, lambda : SparkETLLaunchFailure("dataflow failed to run the application"))
        run = r.data
        run_id = run.id
        print(f"Job launched, run_id = {run_id}")

        while True:
            time.sleep(10)
            r = df_client.get_run(run_id=run.id)
            check_response(r, lambda : SparkETLGetStatusFailure("dataflow failed to get run status"))
            run = r.data
            print(f"Status: {run.lifecycle_state}")
            if run.lifecycle_state in ('FAILED', 'SUCCEEDED'):
                break

