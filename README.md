# Overview

## Goal
spark_etl provide a platform independent way of building spark application.

## Benefit
* Your application can be moved to different spark platform without change or with very little change.

## Supported platform
* Local spark via pyspark package.
* Spark cluster with Livy Interface
* Oracle Dataflow

# Concepts

## Application
An application is the code for a spark job. It contains:
* A `main.py` file which contain the application entry
* A `manifest.json` file, which specify the metadata of the application.
* A `requirements.txt` file, which specify the application dependency.

See [examples/myapp](examples/myapp) for example.

## Build an application
To build an application, run
```
./etl.py -a build --app-dir <app-dir> --build-dir <build-dir>
```
* `<app_dir>` is the directory where your application is located.
* `<build-dir>` is the directory where you want your build to be deployed
    * Your build actually located at `<build-dir>/<version>`, where `<version>` is specified by application's manifest file

* **Build is mostly platform independent. You need to depend on package oci-core if you intent to use oci Dataflow**

## Application entry signature
In your application's `main.py`, you shuold have a `main` function with the following signature:
* `spark` is the spark session object
* `input_args` a dict, is the argument user specified when running this job.
* `sysops` is the system options passed, it is platform specific.
* Your `main` function's return value will be returned from the job submitter to the caller.
```
def main(spark, input_args, sysops={}):
    # your code here
```
See [here](examples/myapp/main.py) for example.

## Deployer
* spark_etl support the following deployer
    * `spark_etl.vendors.local.LocalDeployer`
    * `spark_etl.deployers.HDFSDeployer`
    * `spark_etl.vendors.oracle.DataflowDeployer`

the `etl.py` command use the config file to decide which deployer to use

## Job Submitter
* spark_etl support the following job submitter
    * `spark_etl.vendors.local.PySparkJobSubmitter`
    * `spark_etl.job_submitters.livy_job_submitter.LivyJobSubmitter`
    * `spark_etl.vendors.oracle.DataflowJobSubmitter`
* Job summiter's `run` function returns the retrun value from job's `main` function.

the `etl.py` command use the config file to decide with job submitter to use


## Deploy a job using `etl.py` command: (`examples/etl.py`)
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
