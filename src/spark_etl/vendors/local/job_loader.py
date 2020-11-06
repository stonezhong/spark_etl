import importlib
import argparse
import json
import os
import subprocess
import sys

from pyspark.sql import SparkSession

def _bootstrap():
    parser = argparse.ArgumentParser(description='job')
    parser.add_argument(
        "--run-id", type=str, required=True, help="Run ID",
    )
    parser.add_argument(
        "--run-dir", type=str, required=True, help="Run Directory",
    )
    parser.add_argument(
        "--app-dir", type=str, required=True, help="Application Directory",
    )
    args = parser.parse_args()
    print(f"run-id:  {args.run_id}")
    print(f"run-dir: {args.run_dir}")
    print(f"app-dir: {args.app_dir}")

    run_home = os.path.join(args.run_dir, args.run_id)
    print(f"run-home: {run_home}")
    os.chdir(run_home)

    # setup lib path
    sys.path.insert(0, os.path.join(args.app_dir, 'app.zip'))
    sys.path.insert(0, os.path.join(args.app_dir, 'lib'))

    # change home dir to the deployment location
    os.chdir(args.app_dir)

    # load input args
    with open(os.path.join(args.run_dir, args.run_id, "input.json"), "r") as f:
        input_args = json.load(f)

    spark = SparkSession.builder.appName(args.run_id).getOrCreate()

    try:
        entry = importlib.import_module("main")
        result = entry.main(spark, input_args)

        # save output
        with open(os.path.join(run_home, "result.json"), "w") as out_f:
            json.dump(result, out_f)

    finally:
        spark.stop()

_bootstrap()

