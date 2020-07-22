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
    current_dir = os.getcwd()
    unique_id = str(uuid.uuid4())
    lib_dir = os.path.join(current_dir, 'python_libs')
    bin_dir = os.path.join(lib_dir, unique_id)
    lib_zip = os.path.join(lib_dir, f"{unique_id}.zip")
    os.makedirs(bin_dir, exist_ok=True)

    # lib_url must be a HDFS url (or perhaps can be handled by a hdfs connector)    
    subprocess.check_call(["hdfs", "dfs", "-copyToLocal", lib_url, lib_zip])
    subprocess.check_call(['unzip', lib_zip, "-d", bin_dir])
    sys.path.insert(0, bin_dir)


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

# get input
input_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
input_file.close()
subprocess.check_call(["hdfs", "dfs", "-copyToLocal", "-f", f"{args.run_dir}/input.json", input_file.name])
with open(input_file.name) as f:
    input_args = json.load(f)

spark = SparkSession.builder.appName("RunJob").getOrCreate()

try:
    entry = importlib.import_module("main")
    result = entry.main(spark, input_args)

    # save output
    output_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
    json.dump(result, output_file)
    output_file.close()

    subprocess.check_call(["hdfs", "dfs", "-copyFromLocal", output_file.name, f"{args.run_dir}/result.json"])
    print("job_loader: exit gracefully")
finally:
    spark.stop()
