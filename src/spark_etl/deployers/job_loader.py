import importlib
import argparse
import argparse
import uuid
import os
import subprocess
import sys
import tempfile
import json

from pyspark.sql import SparkSession

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
def _install_libs(lib_url):
    ##########################################
    #
    # |
    # +-- python_libs
    #       |
    #       +-- uuid1.zip
    #       |
    #       +-- uuid2.zip
    #       |
    #       +-- uuid1  (lib extracted)
    #       |
    #       +-- uuid2  (lib extracted)
    ##########################################
    print("job_loader._install_libs: enter, lib_url = {}".format(lib_url))
    current_dir = os.getcwd()
    unique_id = str(uuid.uuid4())
    lib_dir = os.path.join(current_dir, 'python_libs')
    bin_dir = os.path.join(lib_dir, unique_id)
    lib_zip = os.path.join(lib_dir, f"{unique_id}.zip")
    os.makedirs(bin_dir, exist_ok=True)

    # lib_url must be a HDFS url (or perhaps can be handled by a hdfs connector)
    subprocess.check_call(["hdfs", "dfs", "-copyToLocal", lib_url, lib_zip])
    subprocess.check_call(['unzip', '-qq', lib_zip, "-d", bin_dir])
    sys.path.insert(0, bin_dir)
    print("job_loader._install_libs: exit")


print("job_loader.py: enter")

parser = argparse.ArgumentParser(description='job')
parser.add_argument(
    "--run-id", type=str, required=True, help="Run ID",
)
parser.add_argument(
    "--run-dir", type=str, required=True, help="Run Directory",
)
parser.add_argument(
    "--lib-url", type=str, required=True, help="Library URL",
)

args = parser.parse_args()

_install_libs(args.lib_url)

spark = SparkSession.builder.appName("RunJob").getOrCreate()

# get input
server_channel = get_server_channel(args.run_dir)
input_args = server_channel.read_json(spark, "input.json")

try:
    entry = importlib.import_module("main")
    result = entry.main(spark, input_args, sysops={
        "channel": get_server_channel(args.run_dir)
    })

    # save output
    server_channel.write_json(spark, "result.json", result)
    print("job_loader: exit gracefully")
finally:
    spark.stop()
