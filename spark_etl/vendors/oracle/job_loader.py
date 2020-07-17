import argparse
import importlib
import os
import subprocess
import sys
import uuid
from urllib.parse import urlparse

from pyspark.sql import SparkSession, SQLContext, Row

# lib installer
def _install_libs(lib_url):
    current_dir = os.getcwd()
    unique_id = str(uuid.uuid4())
    lib_dir = os.path.join(current_dir, unique_id, 'python_libs')
    lib_zip = os.path.join(current_dir, unique_id, 'lib.zip')
    
    os.makedirs(lib_dir)
    subprocess.check_call(['wget', "-O", lib_zip, lib_url])
    subprocess.check_call(['unzip', lib_zip, "-d", lib_dir])
    sys.path.insert(0, lib_dir)

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
    from oci_core import dfapp_get_os_client, get_delegation_token
    delegation_token = get_delegation_token(spark)
    os_client = dfapp_get_os_client(args.app_region, delegation_token)
    xargs = _get_args(os_client, run_dir)
    entry = importlib.import_module("main")
    result = entry.main(spark, xargs, sysops={
        "install_libs": lambda : _install_libs(args.lib_url)
    })

    _save_result(os_client, run_dir, result)

    # TODO: make a copy of the log file


_bootstrap()
