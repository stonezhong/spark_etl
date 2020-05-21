#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# from spark_etl import ETLEngine
# from livy_job_submitter import LivyJobSubmitter

from spark_etl import Application
from spark_etl.job_submitters.livy_job_submitter import LivyJobSubmitter
from spark_etl.deployers import HDFSDeployer

def main():
    app = Application("/home/stonezhong/DATA_DISK/projects/spark_etl/examples/myapp")
    # app.build("/home/stonezhong/DATA_DISK/projects/spark_etl/examples/myapp/build")

    # let's deploy it
    deployer = HDFSDeployer({
        "bridge": "spnode1",
        "stage_dir": "/root/.stage_dir",
    })

    # deployer.deploy(
    #     "/home/stonezhong/DATA_DISK/projects/spark_etl/examples/myapp/build",
    #     "/apps/myjob/v1.0"
    # )


    job_submitter = LivyJobSubmitter({
        'service_url'   : 'http://10.0.0.11:60008',
        'username'      : 'root',
        'password'      : 'foo',
        'bridge'        : 'spnode1',
    })
    job_submitter.run("/apps/myjob/v1.0")

if __name__ == '__main__':
    main()