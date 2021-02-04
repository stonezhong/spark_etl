import json
from urllib.parse import urlparse
import time
import uuid
from datetime import datetime, timedelta
from termcolor import colored, cprint
import readline

import oci
from oci_core import get_os_client, get_df_client, os_upload, os_upload_json, os_download, os_download_json, os_get_endpoint, \
    os_has_object, os_delete_object

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

    def cli_loop(self, run_id):
        # line_mode can be "bash", "python" or "OFF"
        # When line_mode is OFF, you need send explicitly run @@bash or @@python
        # to submit a block of code to server
        # When line_mode is bash, each line is a bash script
        # when line_mode is python, each line is a python script
        line_mode = "off" 
       
        # if is_waiting_for_response is True, we need to pull server for cli-response.json
        # if is_waiting_for_response is False, we are free to enter new command
        is_waiting_for_response = True      
        # command line buffer
        cli_lines = []
        cli_wait_prompt = "-/|\\"
        cli_wait_prompt_idx = 0
        log_filename = None
        # commands
        # @@log           -- all the output will be written to the log file as well
        #                    by default there is not log
        # @@nolog         -- turn off log
        # @@clear         -- clear command line buffer
        # @@load          -- load a script from local file and append to command buffer
        # @@bash          -- submit a bash script
        # @@python        -- submit a python script
        # @@show          -- show the command buffer
        # @@pwd           -- show driver's current directory
        # @@quit          -- quit the cli console

        command = None
        while True:
            if not is_waiting_for_response:
                if line_mode == "off":
                    prompt = "> "
                elif line_mode == "bash":
                    prompt = "bash> "
                else:
                    prompt = "python> "

                command = input(prompt)

                if command == "@@quit":
                    self.write_cli_request(
                        run_id, 
                        {
                            "type": "@@quit",
                        }
                    )
                    is_waiting_for_response = True
                    continue
                
                if command == "@@pwd":
                    self.write_cli_request(
                        run_id, 
                        {
                            "type": "@@pwd",
                        }
                    )
                    is_waiting_for_response = True
                    continue

                if command == "@@bash":
                    self.write_cli_request(
                        run_id, 
                        {
                            "type": "@@bash",
                            "lines": cli_lines
                        }
                    )
                    is_waiting_for_response = True
                    cli_lines = []
                    continue

                if command == "@@python":
                    self.write_cli_request(
                        run_id, 
                        {
                            "type": "@@python",
                            "lines": cli_lines
                        }
                    )
                    is_waiting_for_response = True
                    cli_lines = []
                    continue
                
                if command.startswith("@@mode"):
                    cmds = command.split(" ")
                    if len(cmds) != 2 or cmds[1] not in ("off", "bash", "python"):
                        print("Usage:")
                        print("@@mode off")
                        print("@@mode python")
                        print("@@mode bash")
                    else:
                        line_mode = cmds[1]
                    continue

                if command.startswith("@@log"):
                    cmds = command.split(" ")
                    if len(cmds) != 2:
                        print("Usage:")
                        print("@@log <filename>")
                    else:
                        log_filename = cmds[1]
                    continue

                if command.startswith("@@load"):
                    cmds = command.split(" ")
                    if len(cmds) != 2:
                        print("Usage:")
                        print("@@load <filename>")
                    else:
                        try:
                            with open(cmds[1], "rt") as load_f:
                                for line in load_f:
                                    cli_lines.append(line.rstrip())
                        except Exception as e:
                            print(f"Unable to read from file: {str(e)}")
                    continue

                if command == "@@clear":
                    cli_lines = []
                    continue

                if command == "@@show":
                    for line in cli_lines:
                        print(line)
                    print()
                    continue

                # for any other command, we will append to the cli buffer
                if line_mode == "off":
                    cli_lines.append(command)
                else:
                    self.write_cli_request(
                        run_id, 
                        {
                            "type": "@@" + line_mode,
                            "lines": [ command ]
                        }
                    )
                    is_waiting_for_response = True
            else:
                if self.has_cli_response(run_id):
                    response = self.read_cli_response(run_id)
                    # print('#################################################')
                    # print('# Response                                      #')
                    # print(f"# status   : {response['status']}")
                    # if 'exit_code' in response:
                    #     print(f"# exit_code: {response['exit_code']}")
                    # print('#################################################')
                    cprint(response['output'], 'green', 'on_red')
                    if log_filename is not None:
                        try:
                            with open(log_filename, "a+t") as log_f:
                                print(response['output'], file=log_f)
                                print("", file=log_f)
                        except Exception as e:
                            print(f"Unable to write to file {log_filename}: {str(e)}")
                    is_waiting_for_response = False
                    if command == "@@quit":
                        break
                else:
                    time.sleep(1) # do not sleep too long since this is an interactive session
                    print(f"\r{cli_wait_prompt[cli_wait_prompt_idx]}\r", end="")
                    cli_wait_prompt_idx = (cli_wait_prompt_idx + 1) % 4



    def run(self, deployment_location, options={}, args={}, handlers=[], on_job_submitted=None, cli_mode=False):
        # options fields
        #     num_executors     : number
        #     driver_shape      :  string
        #     executor_shape    : string
        #     lib_url_duration  : number (repre the number of minutes)
        #     on_job_submitted  : callback, on_job_submitted(run_id, vendor_info={'oci_run_id': 'xxxyyy'})

        # CLI mode allows client to run the spark application as a cli, so the client can send
        # shell script and python script to execute on the driver.
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

            if cli_mode and not cli_entered:
                self.cli_loop(run_id)
                cli_entered = True

        if run.lifecycle_state in ('FAILED', 'CANCELED'):
            raise Exception(f"Job failed with status: {run.lifecycle_state}")
        return self.get_result(run_id)
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

    def get_result(self, run_id):
        result_object_name = f"{self.config['run_base_dir']}/{run_id}/result.json"
        o = urlparse(result_object_name)

        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        object_name = o.path[1:]    # remove the leading "/"

        os_client = get_os_client(self.region, self.config.get("oci_config"))
        result = os_download_json(os_client, namespace, bucket, object_name)
        return result

    def write_cli_request(self, run_id, request):
        os_client = get_os_client(self.region, self.config.get("oci_config"))

        o = urlparse(self.config['run_base_dir'])
        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        root_path = o.path[1:] # remove the leading "/"
        os_upload_json(os_client, request, namespace, bucket, f"{root_path}/{run_id}/cli-request.json")


    def has_cli_response(self, run_id):
        os_client = get_os_client(self.region, self.config.get("oci_config"))

        o = urlparse(self.config['run_base_dir'])
        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        root_path = o.path[1:] # remove the leading "/"
        object_name = f"{root_path}/{run_id}/cli-response.json"

        return os_has_object(os_client, namespace, bucket, f"{root_path}/{run_id}/cli-response.json")

    def read_cli_response(self, run_id):
        os_client = get_os_client(self.region, self.config.get("oci_config"))

        o = urlparse(self.config['run_base_dir'])
        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        root_path = o.path[1:] # remove the leading "/"
        object_name = f"{root_path}/{run_id}/cli-response.json"

        result = os_download_json(os_client, namespace, bucket, object_name)
        os_delete_object(os_client, namespace, bucket, object_name)
        return result


    def kill(self, run_id):
        df_client = get_df_client(self.region)

        r = df_client.delete_run(run_id)
        check_response(r, lambda : SparkETLKillFailure("dataflow failed to kill the run"))
