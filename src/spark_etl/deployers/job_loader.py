import importlib
import argparse
import argparse
import uuid
import os
import subprocess
import sys
import tempfile
import json
import random

from pyspark import SparkFiles
from pyspark.sql import SparkSession

random.seed()

def get_server_channel(run_dir):
    # from spark_etl.core import ServerChannelInterface
    class ServerChannel:
        def __init__(self, run_dir):
            self.run_dir = run_dir

        def read_json(self, spark, name):
            stage_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
            stage_file.close()
            try:
                subprocess.check_call([
                    "hdfs", "dfs", "-copyToLocal", "-f",
                    os.path.join(self.run_dir, name),
                    stage_file.name
                ])
                with open(stage_file.name) as f:
                    return json.load(f)
            finally:
                os.remove(stage_file.name)


        def has_json(self, spark, name):
            exit_code = subprocess.call([
                "hdfs", "dfs", "-test", "-f",
                os.path.join(self.run_dir, name)
            ])
            if exit_code == 0:
                return True
            if exit_code == 1:
                return False
            raise Exception(f"Unrecognized exit_code: {exit_code}")


        def write_json(self, spark, name, payload):
            stage_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
            stage_file.close()
            try:
                with open(stage_file.name, "wt") as f:
                    json.dump(payload, f)
                subprocess.check_call([
                    "hdfs", "dfs", "-copyFromLocal", stage_file.name,
                    os.path.join(self.run_dir, name)
                ])
            finally:
                os.remove(stage_file.name)


        def delete_json(self, spark, name):
            subprocess.check_call([
                "hdfs", "dfs", "-rm", os.path.join(self.run_dir, name)
            ])

    return ServerChannel(run_dir)


# lib installer
def _install_libs(run_id, base_lib_dir):
    ##########################################
    #
    # |
    # +-- {lib_dir}
    #       |
    #       +-- uuid1.zip
    #       |
    #       +-- uuid2.zip
    #       |
    #       +-- uuid1  (lib extracted)
    #       |
    #       +-- uuid2  (lib extracted)
    ##########################################
    print("job_loader._install_libs: enter, run_id = {}".format(run_id))

    if base_lib_dir:
        current_dir = base_lib_dir
    else:
        current_dir = os.getcwd()
    print(f"current_dir = {current_dir}")

    base_dir    = os.path.join(current_dir, run_id)
    lib_dir     = os.path.join(base_dir, 'python_libs')
    lib_zip     = SparkFiles.get("lib.zip")
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
                    subprocess.check_call(['unzip', "-qq", lib_zip, "-d", lib_dir])
                    print("_install_libs: install lib done")
                if lib_dir not in sys.path:
                    print(f"_install_libs: add {lib_dir} path")
                    sys.path.insert(0, lib_dir)
                print("job_loader._install_libs: exit")
                return
            finally:
                os.remove(lock_name)
        except OSError as e:
            if e.errno == errno.EEXIST:
                time.sleep(random.randint(1, 10))
                continue
            raise

    raise Exception("Failed to install libraries!")



print("job_loader.py: enter")

parser = argparse.ArgumentParser(description='job')
parser.add_argument(
    "--run-id", type=str, required=True, help="Run ID",
)
parser.add_argument(
    "--run-dir", type=str, required=True, help="Run Directory",
)
parser.add_argument(
    "--base-lib-dir", type=str, required=False, help="Python library directory for drivers and executors",
)
parser.add_argument(
    "--lib-zip", type=str, required=True, help="Zipped library",
)

args = parser.parse_args()
spark = SparkSession.builder.appName("RunJob").getOrCreate()

sc = spark.sparkContext
sc.addFile(args.lib_zip)

_install_libs(args.run_id, args.base_lib_dir)

# get input
server_channel = get_server_channel(args.run_dir)
input_args = server_channel.read_json(spark, "input.json")

try:
    entry = importlib.import_module("main")
    result = entry.main(spark, input_args, sysops={
        "install_libs": lambda : _install_libs(run_id, args.base_lib_dir),
        "channel": get_server_channel(args.run_dir)
    })

    # save output
    server_channel.write_json(spark, "result.json", result)
    print("job_loader: exit gracefully")
finally:
    spark.stop()
