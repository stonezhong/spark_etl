#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from spark_etl.tools import HDFS, Livy
import json
import os

from spark_etl import ETLEngine
from spark_etl.engine.namespaces import CasperNamespace
from spark_etl.data_registry import DataObjectInfo

def main():
    return
    config_dir = os.path.join(
        os.path.expandvars("$ENV_HOME"), 'configs', 'spark_etl_example'
    )

    with open(os.path.join(config_dir, 'spark_config.json'), 'rb') as f:
        spark_config = json.load(f)


    hdfs = HDFS(
        spark_config["webhdfs"]["service_url"],
        username=spark_config["webhdfs"]["username"], 
        password=spark_config["webhdfs"].get("password"), 
        hdfs_username=spark_config["webhdfs"]["hdfs_username"],
        auth_cert=os.path.join(config_dir,spark_config["webhdfs"]['auth_cert']),
        auth_key=os.path.join(config_dir,spark_config["webhdfs"]['auth_key']),
        ca_cert=os.path.join(config_dir,spark_config["webhdfs"]['ca_cert']),
    ) 

    livy = Livy(
        spark_config["livy"]["service_url"],
        username=spark_config["livy"]["username"], 
        password=spark_config["livy"].get("password"), 
        hdfs_username=spark_config["livy"]["hdfs_username"],
        auth_cert=os.path.join(config_dir,spark_config["livy"]['auth_cert']),
        auth_key=os.path.join(config_dir,spark_config["livy"]['auth_key']),
        ca_cert=os.path.join(config_dir,spark_config["livy"]['ca_cert']),
    )

    submit_args = {
        "file": "/hwd/tmp_shizhong/job_main.py",
        "name": "stonezhong's test",
        "archives": [ "/hwd/tmp_shizhong/oci.zip#oci" ],
        "files": [ '/hwd/tmp_shizhong/spark_etl_config.json' ],
        "pyFiles": [ "/hwd/tmp_shizhong/app.zip", "/hwd/tmp_shizhong/spark_etl.zip" ],
        "conf": {
            "spark.ui.view.acls.groups"  : "hwd-osap-admins",
        },
    }

    ret = livy.submit_job(**submit_args)
    print(ret)

    # create and initialize ETL engine
    # etl_engine = ETLEngine()
    # etl_engine.register_namespace(CasperNamespace("casper_pulse", "oci-pulse-prod"))

    # do_info = DataObjectInfo(
    #     namespace_name='casper_pulse',
    #     path='/IAD/problems/IAD_AD_1/problems/2018/11/1/0/2018-11-01T00:00:18.374Z::bare-metal-stats-01301.node.ad1.us-ashburn-1.json.gz',
    #     format='raw'
    # )
    # do = etl_engine.get_data_object(do_info)
    # do.download('foo.gz')



if __name__ == '__main__':
    main()