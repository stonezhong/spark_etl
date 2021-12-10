from contextlib import contextmanager
import time
import importlib
import argparse
import json
import os
import subprocess
import sys
import random
import errno
import shutil
import tempfile
from urllib.parse import urlparse

from pyspark.sql import SparkSession
from pyspark import SparkFiles

random.seed()

@contextmanager
def lock(name, interval=1, timeout=None):
    start_time = time.time()
    while True:
        if timeout is not None and time.time() - start_time > timeout:
            raise Exception("lock_timeout")
        lock_fh = None
        try:
            lock_fh = os.open(name, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            # what if yield raises OSError?
            yield
            break
        except OSError as e:
            if e.errno == errno.EEXIST:
                time.sleep(interval)
                continue
        finally:
            if lock_fh is not None:
                os.close(lock_fh)
                os.remove(name)

def clean_dir(dirname):
    for fname in os.listdir(dirname):
        full_fname = os.path.join(dirname, fname)
        if os.path.isfile(full_fname):
            os.remove(full_fname)
        elif os.path.isdir(full_fname):
            shutil.rmtree(full_fname)
        else:
            raise Exception("Neither file nor directory")

def getLFSServerChannel(run_home_dir):
    class LFSServerChannel:
        def __init__(self, run_home_dir):
            self.run_home_dir = run_home_dir

        def read_json(self, spark, name):
            with open(os.path.join(self.run_home_dir, name), "r") as f:
                return json.load(f)

        def has_json(self, spark, name):
            return os.path.isfile(os.path.join(self.run_home_dir, name))

        def write_json(self, spark, name, payload):
            with open(os.path.join(self.run_home_dir, name), "w") as f:
                json.dump(payload, f)

        def delete_json(self, spark, name):
            os.remove(os.path.join(self.run_home_dir, name))
    return LFSServerChannel(run_home_dir)

def getHadoopServerChannel(run_home_dir):
    class HadoopServerChannel:
        def __init__(self, run_home_dir):
            self.run_home_dir = run_home_dir

        def get_context(self, spark):
            sc = spark.sparkContext
            FileSystem = sc._jvm.org.apache.hadoop.fs.FileSystem
            Path = sc._jvm.org.apache.hadoop.fs.Path
            URI = sc._jvm.java.net.URI
            fs = FileSystem.get(URI(self.run_home_dir), sc._jsc.hadoopConfiguration())
            return (Path, fs,)
            
        def read_json(self, spark, name):
            Path, fs = self.get_context(spark)
            stage_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
            stage_file.close()
            try:
                full_resource_name = os.path.join(self.run_home_dir, name)
                o = urlparse(full_resource_name)
                fs.copyToLocalFile(False, Path(o.path), Path(stage_file.name))
                with open(stage_file.name) as f:
                    return json.load(f)
            finally:
                os.remove(stage_file.name)

        def has_json(self, spark, name):
            Path, fs = self.get_context(spark)
            full_resource_name = os.path.join(self.run_home_dir, name)
            o = urlparse(full_resource_name)
            return fs.exists(Path(o.path))

        def write_json(self, spark, name, payload):
            Path, fs = self.get_context(spark)
            stage_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
            stage_file.close()
            try:
                with open(stage_file.name, "w") as f:
                    json.dump(payload, f)
                full_resource_name = os.path.join(self.run_home_dir, name)
                o = urlparse(full_resource_name)
                fs.copyFromLocalFile(False, Path(stage_file.name), Path(o.path))
            finally:
                os.remove(stage_file.name)

        def delete_json(self, spark, name):
            Path, fs = self.get_context(spark)
            full_resource_name = os.path.join(self.run_home_dir, name)
            o = urlparse(full_resource_name)
            return fs.delete(Path(o.path), False)

    return HadoopServerChannel(run_home_dir)


# lib installer
def _install_libs(stage_home_dir):
    print(f"job_loader._install_libs: enter, stage_home_dir = {stage_home_dir}")

    lock_name   = os.path.join(stage_home_dir, '__lock__')
    lib_dir     = os.path.join(stage_home_dir, "python_libs")
    lib_zip     = SparkFiles.get("lib.zip")

    with lock(lock_name):
        if not os.path.isdir(lib_dir):
            print("_install_libs: create lib starts")
            os.makedirs(lib_dir)
            subprocess.check_call(['unzip', "-qq", lib_zip, "-d", lib_dir])
            print("_install_libs: create lib done")
        if lib_dir not in sys.path:
            print(f"_install_libs: add {lib_dir} path")
            sys.path.insert(0, lib_dir)
    print("job_loader._install_libs: exit")

def _bootstrap():
    parser = argparse.ArgumentParser(description='job')
    parser.add_argument(
        "--run-id", type=str, required=True, help="Run ID",
    )
    parser.add_argument(
        "--run-dir", type=str, required=True, help="Run Directory",
    )
    parser.add_argument(
        "--lib-zip", type=str, required=True, help="Library Zip File Location",
    )
    parser.add_argument(
        "--spark-host-stage-dir", type=str, required=True, help="Spark Host Stage Directory",
    )
    parser.add_argument(
        "--cli-mode",
        action="store_true",
        help="Using cli mode?"
    )
    args = parser.parse_args()
    spark = SparkSession.builder.appName(f"RunJob-{args.run_id}").getOrCreate()

    ######################################################################################
    # run_id              : a uuid string, it is unique per run
    # run_dir             : a URI, unique per run, a place to put input.json
    #                       and drop result.json, and a palce for server and
    #                       client to share via channels
    # lib_zip             : a URI, points to python lib zip file
    # stage_home_dir      : a unique file path in local file system per run, where 
    #                       we can save temporary files
    ######################################################################################
    stage_home_dir = os.path.join(args.spark_host_stage_dir, args.run_id)
    print("============ configurations ==================")
    print(f"run-id              : {args.run_id}")
    print(f"run-dir             : {args.run_dir}")
    print(f"lib-zip             : {args.lib_zip}")
    print(f"stage-home-dir      : {stage_home_dir}")
    print(f"cli-mode            : {args.cli_mode}")
    print("==============================================")

    sc = spark.sparkContext
    sc.addFile(args.lib_zip)

    os.makedirs(stage_home_dir, exist_ok=True)
    clean_dir(stage_home_dir)

    # setup lib path
    _install_libs(stage_home_dir)

    o = urlparse(args.run_dir)
    if o.scheme in ("s3", "s3a", "hdfs", "file"):
        print("Using HadoopServerChannel")
        server_channel = getHadoopServerChannel(args.run_dir)
    else:
        print("Using LFSServerChannel")
        server_channel = getLFSServerChannel(args.run_dir)

    input_args = server_channel.read_json(spark, "input.json")

    try:
        if args.cli_mode:
            entry = importlib.import_module("spark_etl.utils")
            entry.cli_main(
                spark, 
                input_args, 
                sysops={
                    "install_libs": lambda : _install_libs(stage_home_dir),
                    "channel": server_channel
                }
            )

        entry = importlib.import_module("main")
        result = entry.main(spark, input_args, sysops={
            "install_libs": lambda : _install_libs(stage_home_dir),
            "channel": server_channel
        })
        # save output
        server_channel.write_json(spark, "result.json", result)

    finally:
        spark.stop()

_bootstrap()

