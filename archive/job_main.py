from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext
import subprocess
import json
import sys
sys.path.insert(0, 'oci')

spark = SparkSession.builder.appName("RunJob").getOrCreate()

from spark_etl import ETLEngine
from spark_etl.engine.namespaces import CasperNamespace, HDFSNamespace, JDBCNamespace

def main():
    # open config file
    with open("spark_etl_config.json", "rb") as f:
        config = json.load(f)

    import oci
    # create and initialize ETL engine
    etl_engine = ETLEngine()
    etl_engine.register_namespace(CasperNamespace("casper_pulse", "oci-pulse-prod"))
    etl_engine.register_namespace(HDFSNamespace("hdfs", spark))

    dw_config = config['namespaces']['dw']
    jdbc_namespace = JDBCNamespace('dw', spark, dw_config['username'], dw_config['password'], dw_config['jdbc_url'])
    etl_engine.register_namespace(jdbc_namespace)
    jdbc_namespace.register_query('sample', 'select * from dw_stg_user.hops_status_dim')


    # path='/IAD/problems/IAD_AD_1/problems/2018/11/1/0/2018-11-01T00:00:18.374Z::bare-metal-stats-01301.node.ad1.us-ashburn-1.json.gz'
    # do = etl_engine.get_data_object('casper_pulse', path, 'raw')
    # do.download('foo.gz')
    # subprocess.call(["ls", "-al"])
    # print("Done1")

    # path = '/hwd/data/prod/pulse/problems/raw/2018-11-01/IAD_AD_1_2018-11-01.parquet'
    # do = etl_engine.get_data_object('hdfs', path, 'parquet')
    # do.to_df().show()

    # let's build a pipeline
    pipe = 

    do = etl_engine.get_data_object('dw', '/sample', 'query')
    do.to_df().show()

    print("Done")

main()
