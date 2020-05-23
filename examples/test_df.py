#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from spark_etl import Application
from spark_etl.vendors.oracle import DataflowJobSubmitter, DataflowDeployer

def main():
    APP_DIR     = "/home/opc/projects/spark_etl/examples/myapp"
    BUILD_DIR   = f"{APP_DIR}/build"
    REGION      = "IAD"
    DEPLOY_DIR  = "oci://dataflow-apps@idrnu3akjpv5/hwd/myjob"

    # print("Build application")
    # app = Application(APP_DIR)
    # app.build(BUILD_DIR)

    # print("Deploy application")
    # deployer = DataflowDeployer({
    #     "region": REGION,
    #     "dataflow": {
    #         "compartment_id": "ocid1.tenancy.oc1..aaaaaaaax7td4zfyexbwdz3tvcgsolgtw5okcvmnzpjryfzfgpvoamk74t3a",
    #         "driver_shape": "VM.Standard2.1",
    #         "executor_shape": "VM.Standard2.1",
    #         "num_executors": 1
    #     }
    # })
    # deployer.deploy(BUILD_DIR, DEPLOY_DIR)

    job_submitter = DataflowJobSubmitter({
        "region": REGION,
    })
    job_submitter.run(f"{DEPLOY_DIR}/1.0.0.1", {
        "display_name": "test spark_etl"
    })

if __name__ == '__main__':
    main()