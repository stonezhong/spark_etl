* [Overview](#overview)
    * [Goal](#goal)
    * [Benefit](#benefit)
    * [Application](#application)
    * [Build your application](#build_your_application)
    * [Deploy your application](#deploy_your_application)
    * [Run your application](#run_your_application)
    * [Supported platforms](#supported_platforms)
* [APIs](#apis)
    * [Job Deployer](#job-deployer)
    * [Job Submitter](#job-submitter)

# Overview

## Goal
There are many public clouds provide managed Apache Spark as service, such as databricks, AWS EMR, Oracle OCI DataFlow, see the table below for a detailed list.

However, the way to deploy Spark application and launch Spark application are incompatible between different cloud Spark platforms.

Now with `spark-etl`, you can deploy and launch your Spark application in a standard way.

## Benefit
Your application using `spark-etl` can be deployed and launched from different Spark providers without changing the source code. Please check out the demos in the tables below.

## Application
An application is a python program. It contains:
* A `main.py` file which contain the application entry
* A `manifest.json` file, which specify the metadata of the application.
* A `requirements.txt` file, which specify the application dependency.

### Application entry signature
In your application's `main.py`, you shuold have a `main` function with the following signature:
* `spark` is the spark session object
* `input_args` a dict, is the argument user specified when running this application.
* `sysops` is the system options passed, it is platform specific. Job submitter may inject platform specific object in `sysops` object.
* Your `main` function's return value should be a JSON object, it will be returned from the job submitter to the caller.
```
def main(spark, input_args, sysops={}):
    # your code here
```
[Here](examples/apps/demo01) is an application example.


## Build your application
`etl -a build -c <config-filename> -p <application-name>`

For details, please checkout examples below.

## Deploy your application
`etl -a deploy -c <config-filename> -p <application-name> -f <profile-name>`

For details, please checkout examples below.

## Run your application
`etl -a run -c <config-filename> -p <application-name> -f <profile-name> --run-args <input-filename>`

For details, please checkout examples below.


## Supported platforms
<table>
    <tr>
        <td>
            <img
                src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png"
                width="120px"
            />
        </td>
        <td>You setup your own Apache Spark Cluster.

* [Demo: Access Data on HDFS](examples/livy_hdfs1/readme.md)
* [Demo: Access Data on AWS S3](examples/livy_hdfs2/readme.md)
        </td>
    </tr>
    <tr>
        <td>
            <img src="https://miro.medium.com/max/700/1*qgkjkj6BLVS1uD4mw_sTEg.png" width="120px" />
        </td>
        <td>
            Use <a href="https://pypi.org/project/pyspark/">PySpark</a> package, fully compatible to other spark platform, allows you to test your pipeline in a single computer.
* [Demo: Access Data on local filesystem](examples/pyspark_local/readme.md)
* [Demo: Access Data on AWS S3](examples/pyspark_s3/readme.md)
        </td>
    </tr>
    <tr>
        <td>
            <img src="https://databricks.com/wp-content/uploads/2019/02/databricks-generic-tile.png" width="120px">
        </td>
        <td>You host your spark cluster in <a href="https://databricks.com/">databricks </a></td>
    </tr>
    <tr>
        <td>
            <img
                src="https://blog.ippon.tech/content/images/2019/06/emrlogogo.png"
                width="120px"
            />
        </td>
        <td>You host your spark cluster in <a href="https://aws.amazon.com/emr/">Amazon AWS EMR</a>
* [Demo: Access Data on AWS S3](examples/aws_emr/readme.md)
        </td>
    </tr>
    <tr>
        <td>
            <img
                src="https://d15shllkswkct0.cloudfront.net/wp-content/blogs.dir/1/files/2020/07/100-768x402.jpeg"
                width="120px"
            />
        </td>
        <td>You host your spark cluster in <a href="https://cloud.google.com/dataproc">Google Cloud</a></td>
    </tr>
    <tr>
        <td>
            <img
                src="https://apifriends.com/wp-content/uploads/2018/05/HDInsightsDetails.png"
                width="120px"
            />
        </td>
        <td>You host your spark cluster in <a href="https://azure.microsoft.com/en-us/services/hdinsight/">Microsoft Azure HDInsight</a></td>
    </tr>
    <tr>
        <td>
            <img
                src="https://cdn.app.compendium.com/uploads/user/e7c690e8-6ff9-102a-ac6d-e4aebca50425/d3598759-8045-4b7f-9619-0fed901a9e0b/File/a35b11e3f02caf5d5080e48167cf320c/1_xtt86qweroeeldhjroaaaq.png"
                width="120px"
            />
        </td>
        <td>
            You host your spark cluster in <a href="https://www.oracle.com/big-data/data-flow/">Oracle Cloud Infrastructure, Data Flow Service</a>
        </td>
    </tr>
    <tr>
        <td>
            <img
                src="https://upload.wikimedia.org/wikipedia/commons/2/24/IBM_Cloud_logo.png"
                width="120px"
            />
        </td>
        <td>You host your spark cluster in <a href="https://www.ibm.com/products/big-data-and-analytics">IBM Cloud</a></td>
    </tr>
</table>




# APIs
[pydocs for APIs](https://stonezhong.github.io/spark_etl/pydocs/spark_etl.html)


## Job Deployer
For job deployers, please check the [wiki](https://github.com/stonezhong/spark_etl/wiki#job-deployer-classes) .


## Job Submitter
For job submitters, please check the [wiki](https://github.com/stonezhong/spark_etl/wiki#job-submitter-classes)


