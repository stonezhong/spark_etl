#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import argparse
import json
import importlib
import os

from spark_etl import Application, Build

# Example
#
# Build
# ./etl.py -a build --app-dir ./myapp --build-dir ./myapp/build
#
# Deploy
# ./etl.py -a deploy -c config.json       --build-dir ./myapp/build --deploy-dir hdfs:///etl/apps/myapp
# ./etl.py -a deploy -c config_local.json --build-dir ./myapp/build --deploy-dir $HOME/etl_lab/apps/myapp
# ./etl.py -a deploy -c config_oci.json   --build-dir ./myapp/build --deploy-dir oci://dataflow-apps@idrnu3akjpv5/testapps/myapp
#
# Run the application
# ./etl.py -a run -c config.json       --deploy-dir hdfs:///etl/apps/myapp    --version 1.0.0.1
# ./etl.py -a run -c config_local.json --deploy-dir $HOME/etl_lab/apps/myapp  --version 1.0.0.1
# ./etl.py -a run -c config_oci.json   --deploy-dir oci://dataflow-apps@idrnu3akjpv5/testapps/myapp --version 1.0.0.1
#
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a", "--action", required=True, choices=['build', 'deploy', 'run']
    )
    parser.add_argument(
         "-c", "--config-filename", help="Configuration file"
    )
    parser.add_argument(
        "--app-dir", help="Application directory"
    )
    parser.add_argument(
        "--build-dir", help="Build directory"
    )
    parser.add_argument(
        "--deploy-dir", help="Deployment directory"
    )
    parser.add_argument(
        "--version", help="Application version"
    )
    parser.add_argument(
        "--args", help="Application args"
    )
    parser.add_argument(
        "--cli-mode",
        action="store_true",
        help="Enter cli mode?"
    )
    args = parser.parse_args()
    if args.action == "build":
        do_build(args)
    elif args.action == "deploy":
        do_deploy(args)
    elif args.action == "run":
        do_run(args)

    return

def get_app_args(args):
    if args.args is None:
        return {}
    with open(args.args, "r") as f:
        return json.load(f)

def get_config(args):
    config_filename = args.config_filename or "config.json"
    with open(config_filename, "r") as f:
        return json.load(f)


def get_deployer(deployer_config):
    class_name  = deployer_config['class']
    module      = importlib.import_module('.'.join(class_name.split(".")[:-1]))
    klass       = getattr(module, class_name.split('.')[-1])

    args    = deployer_config.get("args", [])
    kwargs  = deployer_config.get("kwargs", {})
    return klass(*args, **kwargs)


def get_job_submitter(job_submitter_config):
    class_name  = job_submitter_config['class']
    module      = importlib.import_module('.'.join(class_name.split(".")[:-1]))
    klass       = getattr(module, class_name.split('.')[-1])

    args    = job_submitter_config.get("args", [])
    kwargs  = job_submitter_config.get("kwargs", {})
    return klass(*args, **kwargs)


def get_common_requirements():
    common_req_filename = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "common_requirements.txt"
    )
    if not os.path.isfile(common_req_filename):
        return []

    packages = []
    with open(common_req_filename, "rt") as f:
        for line in f:
            if line.startswith("#"):
                continue
            l = line.strip()
            if len(l) > 0:
                packages.append(l)
    return packages


# build an application
def do_build(args):
    app = Application(args.app_dir)
    app.build(args.build_dir, default_libs=get_common_requirements())

def do_deploy(args):
    config = get_config(args)
    deployer = get_deployer(config['deployer'])
    deployer.deploy(args.build_dir, args.deploy_dir)


def do_run(args):
    config = get_config(args)
    job_submitter = get_job_submitter(config['job_submitter'])
    ret = job_submitter.run(
        f"{args.deploy_dir}/{args.version}",
        options=config.get("job_run_options", {}),
        args=get_app_args(args),
        cli_mode=args.cli_mode
    )
    print("return:")
    print(json.dumps(ret, indent=4))

if __name__ == '__main__':
    main()