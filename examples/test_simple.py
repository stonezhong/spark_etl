#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from spark_etl import Application, Build
from spark_etl.deployers import HDFSDeployer
from spark_etl.job_submitters import LivyJobSubmitter

def main():
    APP_DIR     = "/mnt/DATA_DISK/projects/spark_etl/examples/myapp"
    BUILD_DIR   = f"{APP_DIR}/build"
    DEPLOY_DIR  = "/apps/myjob"

    print("Build application")
    app = Application(APP_DIR)
    app.build(BUILD_DIR)

    build = Build(BUILD_DIR)

    print("Deploy application")
    deployer = HDFSDeployer({
        "bridge"   : "spnode1",
        "stage_dir": "/root/.stage_dir",
    })
    deployer.deploy(BUILD_DIR, DEPLOY_DIR)

    job_submitter = LivyJobSubmitter({
        "service_url": "http://10.0.0.11:60008/",
        "username": "root",
        "password": "foo",
        "bridge": "spnode1"
    })
    job_submitter.run(
        f"{DEPLOY_DIR}/{app.version}"
    )

if __name__ == '__main__':
    main()