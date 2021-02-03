# Index
* [Overview](#overview)
    * [Goal](#goal)
    * [Benefit](#benefit)
    * [Supported platforms](#supported_platforms)
* [APIs](#apis)
    * [Application](#application)
        * [Application entry signature](#application-entry-signature)

See [https://stonezhong.github.io/spark_etl/](https://stonezhong.github.io/spark_etl/) for more informaion

# Overview

## Goal
spark_etl provide a platform independent way of building spark application.

## Benefit
Your application deployed and running using spark-etl is spark provider agnostic. Which means, for example, you can move your application from Azure HDInsight to AWS EMR without changing your application's code.

## Supported platforms
<table>
    <tr>
        <td>
            <img
                src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png"
                width="120px"
            />
        </td>
        <td>You setup your own Apache Spark Cluster.</td>
    </tr>
    <tr>
        <td>
            <img src="https://miro.medium.com/max/700/1*qgkjkj6BLVS1uD4mw_sTEg.png" width="120px" />
        </td>
        <td>
            Use <a href="https://pypi.org/project/pyspark/">PySpark</a> package, fully compatible to other spark platform, allows you to test your pipeline in a single computer.
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
        <td>You host your spark cluster in <a href="https://aws.amazon.com/emr/">Amazon AWS EMR</a></td>
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
## Application
An application is a pyspark application, so far we only support pyspark, Java and Scala support will be added latter. An application contains:
* A `main.py` file which contain the application entry
* A `manifest.json` file, which specify the metadata of the application.
* A `requirements.txt` file, which specify the application dependency.

[Application](src/spark_etl/application.py) class:
* You can create an application via `Application(app_location)`
* You can build an application via `app.build(destination_location)`

### Application entry signature
In your application's `main.py`, you shuold have a `main` function with the following signature:
* `spark` is the spark session object
* `input_args` a dict, is the argument user specified when running this job.
* `sysops` is the system options passed, it is platform specific. Job submitter may inject platform specific object in `sysops` object.
* Your `main` function's return value will be returned from the job submitter to the caller.
```
def main(spark, input_args, sysops={}):
    # your code here
```
[Here](examples/myapp) is an example.

## Job Deployer
* A job deployer has method `deploy(build_dir, destination_location)`, it deploy the application to the destination location
* spark_etl support the following deployer
    * `spark_etl.vendors.local.LocalDeployer`
    * `spark_etl.deployers.HDFSDeployer`
    * `spark_etl.vendors.oracle.DataflowDeployer`


## Job Submitter
* A job submitter has method `run(deployment_location, options={}, args={}, handlers=[], on_job_submitted=None)`, it submit a deployed job

* spark_etl support the following job submitter
    * `spark_etl.vendors.local.PySparkJobSubmitter`
    * `spark_etl.job_submitters.livy_job_submitter.LivyJobSubmitter`
    * `spark_etl.vendors.oracle.DataflowJobSubmitter`
* Job summiter's `run` function returns the retrun value from job's `main` function.


# Tool: etl.py
## Build an application
To build an application, run
```
./etl.py -a build --app-dir <app-dir> --build-dir <build-dir>
```
* `<app_dir>` is the directory where your application is located.
* `<build-dir>` is the directory where you want your build to be deployed
    * Your build actually located at `<build-dir>/<version>`, where `<version>` is specified by application's manifest file

* **Build is mostly platform independent. You need to depend on package oci-core if you intent to use oci Dataflow**


## Deplay an application
```
./etl.py -a deploy \
    -c <config-filename> \
    --build-dir <build-dir> \
    --deploy-dir <deploy-dir>
```

* `-c <config-filename>`: this option specify the config file to use for the deployment
* `--build-dir <build-dir>`: this option specify where to look for the build bits to deploy
* `--deolpy-dir <deploy-dir>`: this option specify what is the destination for the deployment

## Run a job
```
./etl.py -a run \
    -c <config-filename> \
    --deploy-dir <deploy-dir> \
    --version <version> \
    --args <input-json-file>
```
* `-c <config-filename>`: this option specify the config file
* `--build-dir <build-dir>`: this option specify where to look for the build bits to run
* `--version <version>`: this option specify which version of the app to run
* `--args <input-json-file>`: optional parameter for input variable for the job. The `<input-json-file>` points to a json file, the value of the file will be passed to job's main function in `input_args` parameter. If this option is missing, the `input_args` will be set to `{}` when calling the `main` function of the job.
* It prints the return value of the `main` function of the job

# Examples
* [Build, deploy and run in local spark](examples/test-local-spark.md)
* [Build, deploy and run in spark cluster](examples/test-native-spark.md)
* [Build, deploy and run in OCI Dataflow](examples/test-oci-dataflow.md)
