#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from spark_etl import ETLEngine
from livy_job_submitter import LivyJobSubmitter

def main():
    job_submitter = LivyJobSubmitter(
        'http://seavh1.deepspace.local:18003',
        'root',
        'foo'
    )
    engine = ETLEngine(job_submitter)
    engine.submit_job()

if __name__ == '__main__':
    main()