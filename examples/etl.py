#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import argparse
import json
import importlib

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


# build an application
def do_build(args):
    app = Application(args.app_dir)
    app.build(args.build_dir)

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
        args=get_app_args(args)
    )
    print("return:")
    print(json.dumps(ret, indent=4))

if __name__ == '__main__':
    main()