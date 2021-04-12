#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# Tool to generate config for etl.py
import json
import argparse

def parse_ssh_config(args):
    if args.ssh_config is None:
        ssh_config_filename = os.path.expanduser("~/.ssh/config")
    else:
        ssh_config_filename = args.ssh_config
    with open(ssh_config_filename, "rt") as f:
        ssh_config = f.read()

    ssh_keys = {}
    if args.ssh_key is not None:
        for ssh_kv in args.ssh_key:
            ssh_key_name, ssh_key_filename = ssh_kv.split('=')
            with open(ssh_key_filename, "rt") as f:
                ssh_key_content = f.read()
            ssh_keys[ssh_key_name] = ssh_key_content

    return {
        "config": ssh_config,
        "keys": ssh_keys
    }

def configure_hdfs_deployer(args):
    if args.bridge is None:
        raise Exception("missing --bridge")
    if args.stage_dir is None:
        raise Exception("missing --stage-dir")

    ssh_config = parse_ssh_config(args)

    config = {
        "class": "spark_etl.deployers.hdfs_deployer.HDFSDeployer",
        "args": [
            {
                "bridge": args.bridge,
                "stage_dir": args.stage_dir,
                "ssh_config": ssh_config
            }
        ]
    }
    return config

def configure_s3_deployer(args):
    if args.aws_account is None:
        if args.aws_access_key_id is None:
            raise Exception("Without --aws-count, you must provide --aws-access-key-id")
        else:
            aws_access_key_id = args.aws_access_key_id

        if args.aws_secret_access_key is None:
            raise Exception("Without --aws-count, you must provide --aws-secret-access-key")
        else:
            aws_secret_access_key = args.aws_secret_access_key
    else:
        with open(args.aws_account, "rt") as f:
            aws_account = json.load(f)
            aws_access_key_id       = aws_account['aws_access_key_id']
            aws_secret_access_key   = aws_account['aws_secret_access_key']

    config = {
        "class": "spark_etl.deployers.s3_deployer.S3Deployer",
        "args": [
            {
                "aws_access_key_id"     : aws_access_key_id,
                "aws_secret_access_key" : aws_secret_access_key
            }
        ]
    }
    return config

def configure_oci_dataflow_deployer(args):
    config = {
        "class": "spark_etl.vendors.oracle.DataflowDeployer",
        "args": [
            {
                "region"                : args.oci_region,
                "dataflow": {
                    "compartment_id"    : args.oci_compartment_id,
                    "driver_shape"      : args.oci_driver_shape or "VM.Standard2.1",
                    "executor_shape"    : args.oci_executor_shape or "VM.Standard2.1",
                    "num_executors"     : args.oci_num_executors or 1
                }
            }
        ]
    }
    return config



def configure_local_deployer(args):
    config = {
        "class": "spark_etl.vendors.local.local_deployer.LocalDeployer",
        "args": [
            {
            }
        ]
    }
    return config

def configure_livy_job_submitter(args):
    if args.bridge is None:
        raise Exception("missing --bridge")
    if args.stage_dir is None:
        raise Exception("missing --stage-dir")
    if args.run_dir is None:
        raise Exception("missing --run-dir")

    ssh_config = parse_ssh_config(args)

    livy_cfg = {
        'host': args.livy_host,
        'port': args.livy_port,
        'protocol': args.livy_protocol,
        'via_tunnel': args.livy_via_tunnel,
    }
    if args.livy_username is not None:
        livy_cfg['username'] = args.livy_username
        livy_cfg['password'] = args.livy_password
    config = {
        "class": "spark_etl.job_submitters.livy_job_submitter.LivyJobSubmitter",
        "args": [{
            "livy": livy_cfg,
            "bridge": args.bridge,
            "stage_dir": args.stage_dir,
            "run_dir": args.run_dir,
            "ssh_config": ssh_config
        }],
    }
    return config

def configure_pyspark_job_sumbitter(args):
    config = {
        "class": "spark_etl.vendors.local.pyspark_job_submitter.PySparkJobSubmitter",
        "args": [{
            "run_dir": args.run_dir,
            "enable_aws_s3": args.enable_aws_s3
        }],
    }
    config["args"][0]["enable_aws_s3"] = not not args.enable_aws_s3
    if config["args"][0]["enable_aws_s3"]:
        if args.aws_account is not None:
            config["args"][0]["aws_account"] = args.aws_account
    return config

def configure_oci_dataflow_job_sumbitter(args):
    config = {
        "class": "spark_etl.vendors.oracle.DataflowJobSubmitter",
        "args": [{
            "region" : args.oci_region,
            "run_dir": args.run_dir,
        }],
    }
    return config

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--deployer", required=True, choices=[
        'HDFSDeployer', 'S3Deployer', 'LocalDeployer', 'DataflowDeployer',
    ])
    parser.add_argument("--submitter", required=True, choices=[
        'LivyJobSubmitter', 'PySparkJobSubmitter', 'DataflowJobSubmitter',
    ])
    parser.add_argument("--bridge",     help="Bridge host")
    parser.add_argument("--stage-dir",  help="Stage directory")
    parser.add_argument("--run-dir",    help="Run directory")
    parser.add_argument("--ssh-config", help="SSH config filename")
    parser.add_argument(
        "--ssh-key",
        nargs="+",
        metavar="KEY=VALUE",
        help="SSH keys"
    )

    parser.add_argument("--livy-host",     help="Livy hostname or ip")
    parser.add_argument(
        "--livy-port",
        help="Livy port",
        type=int,
        default=8998
    )
    parser.add_argument(
        "--livy-protocol", help="Livy protocol",
        choices=['http', 'https'],
        default='http'
    )
    parser.add_argument("--livy-username",  help="Livy username")
    parser.add_argument("--livy-password",  help="Livy password")
    parser.add_argument(
        "--livy-via-tunnel",
        help="Talk to livy via ssh tunnel?",
        action="store_true",
    )

    parser.add_argument("--aws-account",            help="AWS account json file")
    parser.add_argument("--aws-access-key-id",      help="AWS access key id")
    parser.add_argument("--aws-secret-access-key",  help="AWS secret access key")

    # allow pyspark to access aws-s3 buckets
    parser.add_argument(
        "--enable-aws-s3",
        help="Allow pyspark to access aws s3 buckets",
        action="store_true",
    )

    parser.add_argument("--oci-region",                    help="OCI Region for the app")
    parser.add_argument("--oci-compartment-id",            help="OCI Compartment ID")
    parser.add_argument("--oci-driver-shape",              help="OCI Driver Shape")
    parser.add_argument("--oci-executor-shape",            help="OCI Executor Shape")
    parser.add_argument("--oci-num-executors", type=int,   help="OCI Number of Executors")

    args = parser.parse_args()

    # parse deployer
    if args.deployer == "HDFSDeployer":
        deployer = configure_hdfs_deployer(args)
    elif args.deployer == "S3Deployer":
        deployer = configure_s3_deployer(args)
    elif args.deployer == "LocalDeployer":
        deployer = configure_local_deployer(args)
    elif args.deployer == "DataflowDeployer":
        deployer = configure_oci_dataflow_deployer(args)

    if args.submitter == "LivyJobSubmitter":
        submitter = configure_livy_job_submitter(args)
    elif args.submitter == "PySparkJobSubmitter":
        submitter = configure_pyspark_job_sumbitter(args)
    elif args.submitter == "DataflowJobSubmitter":
        submitter = configure_oci_dataflow_job_sumbitter(args)

    job_run_options_livy = {
        "conf": {
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "python3",
            "spark.executorEnv.PYSPARK_PYTHON": "python3"
        }
    }
    job_run_options_oci = {
        "display_name": "test"
    }

    config = {
        "deployer": deployer,
        "job_submitter": submitter,
        "job_run_options": {}
    }
    if args.submitter in ("LivyJobSubmitter", "PySparkJobSubmitter"):
        config['job_run_options'] = job_run_options_livy
    if args.submitter in ("DataflowJobSubmitter"):
        config['job_run_options'] = job_run_options_oci

    print(json.dumps(config, indent=2))


if __name__ == '__main__':
    main()

