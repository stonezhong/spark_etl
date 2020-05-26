import uuid
import os
import subprocess
from urllib.parse import urlparse
import json
import tempfile

import oci

from spark_etl.deployers import AbstractDeployer
from spark_etl import Build
from .tools import check_response, remote_execute, dump_json, get_os_client, get_df_client, os_upload


class DataflowDeployer(AbstractDeployer):
    def __init__(self, config):
        super(DataflowDeployer, self).__init__(config)
        # config must have
        #   region, e.g., value is IAD
        #   compartment_id: the compartment we want to have the dataflow app
        #   driver_shape
        #   executor_shape
        #   num_executors
    
    @property
    def region(self):
        return self.config["region"]

    def create_application(self, manifest, destination_location):
        df_client = get_df_client(self.region)
        dataflow = self.config['dataflow']

        create_application_details = oci.data_flow.models.CreateApplicationDetails(
            compartment_id=dataflow['compartment_id'],
            display_name=f"{manifest['display_name']}-{manifest['version']}",
            driver_shape=dataflow['driver_shape'],
            executor_shape=dataflow['executor_shape'],
            num_executors=dataflow['num_executors'],
            spark_version="2.4.4",
            file_uri=f"{destination_location}/{manifest['version']}/main.py",
            archive_uri=f"{destination_location}/{manifest['version']}/lib.zip",
            language="PYTHON",
        )

        r = df_client.create_application(
            create_application_details=create_application_details
        )
        check_response(r)
        # TODO: check if the app with the same version already exist
        return r.data

    def deploy(self, build_dir, destination_location):
        o = urlparse(destination_location)
        if o.scheme != 'oci':
            raise Exception("destination_location must be in OCI")
        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        root_path = o.path[1:]    # remove the leading "/"

        build = Build(build_dir)

        print("Uploading files:")
        os_client = get_os_client(self.region)
        for artifact in build.artifacts:
            os_upload(
                os_client, 
                f"{build_dir}/{artifact}", 
                namespace, 
                bucket, 
                f"{root_path}/{build.version}/{artifact}"
            )

        application = self.create_application(build.manifest, destination_location)
        app_info = {
            "application_id": application.id,
            "compartment_id": application.compartment_id
        }

        deployment_filename = dump_json(app_info)
        os_upload(
            os_client, 
            deployment_filename, 
            namespace, 
            bucket, 
            f"{root_path}/{build.version}/deployment.json"
        )
