#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# from spark_etl import ETLEngine
# from livy_job_submitter import LivyJobSubmitter

from spark_etl import Application
from spark_etl.job_submitters.livy_job_submitter import LivyJobSubmitter
from spark_etl.job_submitters.oci_dataflow_job_submitter import OCIDataflowJobSubmitter
from spark_etl.deployers import HDFSDeployer, OCIDataflowDeployer


def main():
    APP_DIR   = "/home/opc/projects/spark_etl/examples/myapp"
    BUILD_DIR = f"{APP_DIR}/build"

    app = Application(APP_DIR)
    # app.build(BUILD_DIR)

    # let's deploy it
    # deployer = OCIDataflowDeployer({
    #     "region": "IAD",
    # })
    # deployer.deploy(BUILD_DIR)

    # job_submitter = LivyJobSubmitter({
    #     'service_url'   : 'http://10.0.0.11:60008',
    #     'username'      : 'root',
    #     'password'      : 'foo',
    #     'bridge'        : 'spnode1',
    # })
    # job_submitter = LivyJobSubmitter({
    #     'service_url'   : 'http://144.25.100.163:8998',
    #     'username'      : 'root',
    #     'password'      : 'foo',
    #     'bridge'        : 'spc-ad1-1-a',
    # })
    # job_submitter.run("/apps/myjob/v1.0")

    job_submitter = OCIDataflowJobSubmitter({
        'region'        : 'IAD'
    })
    job_submitter.run("oci://dataflow-apps@idrnu3akjpv5/hwd/myjob/1.0.0.1")

if __name__ == '__main__':
    main()