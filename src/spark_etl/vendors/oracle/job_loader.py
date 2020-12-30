import argparse
import importlib
import os
import errno
import subprocess
import sys
import uuid
import json
from urllib.parse import urlparse
import tempfile
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, SQLContext, Row

{% if use_instance_principle %}
USE_INSTANCE_PRINCIPLE = True
{% else %}
USE_INSTANCE_PRINCIPLE = False
OCI_CONFIG = json.loads("""{{ oci_config_str }}""")
OCI_KEY = """{{ oci_key }}"""
{% endif %}


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
    })

    # user need to initialize ask with ask.initialize(spark) before using it

    _save_result(os_client, run_dir, result)

    # TODO: make a copy of the log file


_bootstrap()
