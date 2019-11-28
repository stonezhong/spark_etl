#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import pandas as pd
import numpy as np
from pandasql import sqldf

def main2():
    # lfs_ns = LFSNamespace('file:///home/stonezhong/DATA_DISK/projects/spark_etl/examples/states.csv')

    lfs_ns = LFSNamespace('file:///home/stonezhong/DATA_DISK')
    do = lfs_ns.get('/projects/spark_etl/examples/states.csv', 'csv')
    driver.load(do)



    # states = pd.read_csv('/home/stonezhong/DATA_DISK/projects/spark_etl/examples/states.csv')

    # df = sqldf("SELECT * FROM states", {'states': states})
    # print(df.__class__)
    # print(df)

from spark_etl.engine.pipeline import Valve, ValvePort


def main3():
    from spark_etl.engine.driver.mysql import MySQLDriver
    from spark_etl.engine.namespaces import SQLNamespace
    import mysql.connector

    driver = MySQLDriver(config={
        'user': 'stonezhong',
        'password': 'foobar',
        'host': 'seavh4.deepspace.local',
        'database': 'etl_demo',
        'port': 28001,
    })
    driver.connect()

    sql_ns = SQLNamespace('sql')
    do1 = sql_ns.create_do(
        '/etl_demo/states', 
        type='table', db_name='etl_demo', table_name='states'
    )
    do2 = sql_ns.create_do(
        '/:query/get_info',
        type='query', query_name='get_info', 
        query='SELECT id, name from etl_demo.states where states.population >= 100'
    )
    do3 = sql_ns.create_do(
        '/test/state_info', 
        type='table', db_name='test', table_name='state_info'
    )

    driver.append_to(do2, do3)

    # # build a pipeline
    # v1 = MyComputeValve1()
    # v2 = MyComputeValve2()
    # v1.op_dict['output'].connect(v2.ip_dict['input'])
    # v1.op_dict['output'].emit(do)



def main():
    import mysql.connector
    import pandas.io.sql as psql

    cnx = mysql.connector.connect(
        user     = "stonezhong",
        password = "foobar",
        host     = "seavh4.deepspace.local",
        database = "etl_demo",
        port     = 28001
    )

    df = psql.read_sql("SELECT * FROM etl_demo.states", con=cnx)
    print(df)


if __name__ == '__main__':
    main()
    