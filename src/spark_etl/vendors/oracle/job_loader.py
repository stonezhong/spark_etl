import argparse
import importlib
import os
import io
import errno
import subprocess
import sys
import uuid
import json
from urllib.parse import urlparse
import tempfile
import time
from datetime import datetime, timedelta
from contextlib import redirect_stdout, redirect_stderr
import code

from pyspark.sql import SparkSession, SQLContext, Row

{% if use_instance_principle %}
USE_INSTANCE_PRINCIPLE = True
{% else %}
USE_INSTANCE_PRINCIPLE = False
OCI_CONFIG = json.loads("""{{ oci_config_str }}""")
OCI_KEY = """{{ oci_key }}"""
{% endif %}

class Channel:
    def __init__(self, region, run_dir):
        self.spark = None
        self.region = region
        self.run_dir = run_dir

        o = urlparse(run_dir)
        self.namespace = o.netloc.split('@')[1]
        self.bucket = o.netloc.split('@')[0]
        self.object_name_prefix = o.path[1:]  # drop the leading /

    def bind(self, spark):
        self.spark = spark
    
    def read_json(self, name):
        if self.spark is None:
            raise Exception("Channel: please bind first")
        from oci_core import os_download_json
        os_client = get_os_client_ex(self.spark, self.region)
        return os_download_json(
            os_client, 
            self.namespace, self.bucket, 
            f"{object_name_prefix}/{name}"
        )

    def has_json(self, name):
        if self.spark is None:
            raise Exception("Channel: please bind first")
        from oci_core import os_has_object
        os_client = get_os_client_ex(self.spark, self.region)
        return os_has_object(
            os_client, 
            self.namespace, self.bucket, 
            f"{object_name_prefix}/{name}"
        )
    
    def write_json(self, name, payload):
        if self.spark is None:
            raise Exception("Channel: please bind first")
        from oci_core import os_upload_json
        os_client = get_os_client_ex(self.spark, self.region)
        os_upload_json(
            os_client, 
            payload, 
            self.namespace, self.bucket, 
            f"{object_name_prefix}/{name}"
        )
    
    def delete_json(self, name):
        if self.spark is None:
            raise Exception("Channel: please bind first")
        from oci_core import os_delete_object
        os_client = get_os_client_ex(self.spark, self.region)
        os_delete_object(
            os_client, 
            self.namespace, self.bucket, 
            f"{object_name_prefix}/{name}"
        )
    
# lib installer
def _install_libs(lib_url, run_id):
    current_dir = os.getcwd()
    base_dir    = os.path.join(current_dir, run_id)
    lib_dir     = os.path.join(base_dir, 'python_libs')
    lib_zip     = os.path.join(base_dir, 'lib.zip')
    lock_name   = os.path.join(base_dir, '__lock__')

    os.makedirs(base_dir, exist_ok=True)

    for i in range(0, 100):
        try:
            lock_fh = os.open(lock_name, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(lock_fh)
            try:
                if not os.path.isdir(lib_dir):
                    print("_install_libs: install lib starts")
                    os.makedirs(lib_dir)
                    subprocess.check_call(['wget', "-O", lib_zip, lib_url])
                    subprocess.check_call(['unzip', lib_zip, "-d", lib_dir])
                    print("_install_libs: install lib done")
                if lib_dir not in sys.path:
                    print(f"_install_libs: add {lib_dir} path")
                    sys.path.insert(0, lib_dir)
                return 
            finally:
                os.remove(lock_name)
        except OSError as e:
            if e.errno == errno.EEXIST:
                time.sleep(10)
                continue
            raise
    
    raise Exception("Failed to install libraries!")

class Asker:
    def __init__(self, run_dir, app_region):
        self.spark = None
        self.run_dir = run_dir
        self.app_region = app_region
    
    def initialize(self, spark):
        from oci_core import dfapp_get_os_client, get_delegation_token, get_os_client

        if USE_INSTANCE_PRINCIPLE:
            delegation_token = get_delegation_token(spark)
            self.os_client = dfapp_get_os_client(self.app_region, delegation_token)
        else:
            with tempfile.NamedTemporaryFile(mode='w+t', delete=False) as key_f:
                key_f.write(OCI_KEY)
            _oci_config = dict(OCI_CONFIG)
            _oci_config['key_file'] = key_f.name
            self.os_client = get_os_client(None, config=_oci_config)

    # place a request to the launcher
    def __call__(self, content, timeout=timedelta(minutes=10)):
        from oci_core import os_upload_json, os_download_json

        request_id = str(uuid.uuid4())

        o = urlparse(self.run_dir)
        namespace = o.netloc.split('@')[1]
        bucket = o.netloc.split('@')[0]
        object_name_prefix = o.path[1:]  # drop the leading /

        os_upload_json(self.os_client, content, namespace, bucket, f"{object_name_prefix}/to_submitter/ask_{request_id}.json")
        answer_name = f"{object_name_prefix}/to_submitter/answer_{request_id}.json"
        start_time = datetime.utcnow()
        while True:
            r = self.os_client.list_objects(
                namespace, 
                bucket,
                prefix = answer_name,
                limit = 1
            )
            if len(r.data.objects) == 0:
                if (datetime.utcnow() - start_time) >= timeout:
                    raise Exception("Ask is not answered: timed out")
                time.sleep(5)
                continue

            return os_download_json(self.os_client, namespace, bucket, answer_name)


def _save_result(os_client, run_dir, result):
    from oci_core import os_upload_json
    o = urlparse(run_dir)

    namespace = o.netloc.split('@')[1]
    bucket = o.netloc.split('@')[0]
    object_name_prefix = o.path[1:]  # drop the leading /

    os_upload_json(os_client, result, namespace, bucket, f"{object_name_prefix}/result.json")

def get_os_client_ex(spark, region):
    from oci_core import dfapp_get_os_client, get_delegation_token, get_os_client
    if USE_INSTANCE_PRINCIPLE:
        delegation_token = get_delegation_token(spark)
        os_client = dfapp_get_os_client(region, delegation_token)
    else:
        with tempfile.NamedTemporaryFile(mode='w+t', delete=False) as key_f:
            key_f.write(OCI_KEY)
        _oci_config = dict(OCI_CONFIG)
        _oci_config['key_file'] = key_f.name
        os_client = get_os_client(None, config=_oci_config)
    return os_client



def _get_args(os_client, run_dir):
    from oci_core import os_download_json
    o = urlparse(run_dir)

    namespace = o.netloc.split('@')[1]
    bucket = o.netloc.split('@')[0]
    object_name_prefix = o.path[1:]  # drop the leading /

    return os_download_json(os_client, namespace, bucket, f"{object_name_prefix}/args.json")


def _bootstrap():
    parser = argparse.ArgumentParser(description='job')
    parser.add_argument(
        "--deployment-location", type=str, required=True, help="Deployment Location",
    )
    parser.add_argument(
        "--run-id", type=str, required=True, help="Run ID",
    )
    parser.add_argument(
        "--run-dir", type=str, required=True, help="Run Directory",
    )
    parser.add_argument(
        "--lib-url", type=str, required=True, help="Library URL",
    )
    parser.add_argument(
        "--app-region", type=str, required=True, help="Application Region",
    )
    args = parser.parse_args()

    o = urlparse(args.run_dir)
    if o.scheme != 'oci':
        raise Exception("run_dir MUST be in OCI")

    # Load application
    spark = SparkSession.builder.appName("RunJob").getOrCreate()
    sc = spark.sparkContext
    sc.addPyFile(f"{args.deployment_location}/app.zip")
    print("Application loaded")

    subprocess.check_call(['wget', args.lib_url])

    # The archive file goes into /opt/dataflow
    # Load python libraries
    current_dir = os.getcwd()
    lib_dir = os.path.join(current_dir, 'python_libs')
    os.mkdir(lib_dir)
    subprocess.call([
        'unzip', "lib.zip", "-d", lib_dir
    ])
    sys.path.insert(0, lib_dir)

    run_id = args.run_id
    run_dir = args.run_dir

    # The entry is in a python file "main.py"
    from oci_core import dfapp_get_os_client, get_delegation_token, get_os_client
    if USE_INSTANCE_PRINCIPLE:
        delegation_token = get_delegation_token(spark)
        os_client = dfapp_get_os_client(args.app_region, delegation_token)
    else:
        with tempfile.NamedTemporaryFile(mode='w+t', delete=False) as key_f:
            key_f.write(OCI_KEY)
        _oci_config = dict(OCI_CONFIG)
        _oci_config['key_file'] = key_f.name
        os_client = get_os_client(None, config=_oci_config)
    xargs = _get_args(os_client, run_dir)
    entry = importlib.import_module("main")
    result = entry.main(spark, xargs, sysops={
        "install_libs": lambda : _install_libs(args.lib_url, run_id),
        "ask": Asker(run_dir, args.app_region),
        "cli_main": cli_main,
        "channel": Channel(args.app_region, run_dir)
    })

    # user need to initialize ask with ask.initialize(spark) before using it

    _save_result(os_client, run_dir, result)

    # TODO: make a copy of the log file

class PySparkConsole(code.InteractiveInterpreter):
    def __init__(self, locals=None):
        super(PySparkConsole, self).__init__(locals=locals)

def handle_pwd(user_input, channel):
    channel.write_json(
        "cli-response.json", 
        {
            "status": "ok",
            "output": os.getcwd()
        }
    )

def handle_bash(user_input, channel):
    cmd_buffer = '\n'.join(user_input['lines'])
    f = io.StringIO()
    with redirect_stdout(f):
        p = subprocess.run(cmd_buffer, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
    channel.write_json(
        "cli-response.json", 
        {
            "status": "ok",
            "exit_code": p.returncode,
            "output": p.stdout.decode('utf-8'),
        }
    )

def handle_python(user_input, console, channel):
    source = '\n'.join(user_input['lines'])
    stdout_f = io.StringIO()
    stderr_f = io.StringIO()
    with redirect_stdout(stdout_f):
        with redirect_stderr(stderr_f):
            console.runsource(source, symbol="exec")
    
    channel.write_json(
        "cli-response.json", 
        {
            "status": "ok",
            "output": stdout_f.getvalue() + "\n" + stderr_f.getvalue() ,
        }
    )

def cli_main(spark, args, sysops={}):
    channel = sysops['channel']
    channel.bind(spark)
    console = PySparkConsole(locals={'spark': spark})

    write_json(
        "cli-response.json", 
        {
            "status": "ok",
            "output": "Welcome to OCI Spark-CLI Interface",
        }
    )

    while True:
        if not channel.has_json('cli-request.json'):
            time.sleep(1)
            continue

        user_input = channel.read_json('cli-request.json')
        channel.delete_json('cli-request.json')

        if user_input["type"] == "@@quit":
            channel.write_json(
                "cli-response.json", 
                {
                    "status": "ok",
                    "output": "Server quit gracefully",
                }
            )
            break
        if user_input["type"] == "@@pwd":
            handle_pwd(user_input, channel)
            continue
        if user_input["type"] == "@@bash":
            handle_bash(user_input, channel)
            continue
        if user_input["type"] == "@@python":
            handle_python(user_input, console, channel)
            continue
    return {"status": "ok"}


_bootstrap()
