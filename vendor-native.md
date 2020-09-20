# Deployers
## Scenario
* You have a native Spark Cluster
* You have a livy interface
* You have a Linux host in the spark cluster which you can ssh to as a bridge

## To build the application
See [etl.pl](examples/etl.py). Here is the sample code to build your application:

```
from spark_etl import Application
...

    app = Application("examples/myapp")
    app.build("builds/myapp")
```
In the example abobe:
* Your application code is located at directory `examples/myapp`
* It will build your application and generate artifacts in directory `builds/myapp`
* Your application may have a `requirements.txt` to specify the dependency
* Your application should have a `manifest.json` to specify the version and other metadata


## HDFSDeployer
See [etl.pl](examples/etl.py). Here is the sample code to build your application:
```
from spark_etl.deployers import HDFSDeployer
...

    deployer = HDFSDeployer({
        "bridge"   : "sphost1",
        "stage_dir": "/root/.stage",
    })
    deployer.deploy("builds/myapp", "hdfs://etl/apps/myapp")
```
In the example above:
* It will pick the application artifacts from directory `builds/myapp`
* It will deploy artifacts to `hdfs://etl/apps/myapp/1.0.0.1`, assuming application version is 1.0.0.1 in the manifest
* The way it copy the artifacts to HDFS, it first copy to the bridge host `sphost1`, assuming ssh is already configured (check your ~/.ssh/config), it uses `/root/.stage` on the `spnode1` to store temporary file


## LivyJobSubmitters
See [etl.pl](examples/etl.py). Here is the sample code to build your application:
```
    job_submitter = LivyJobSubmitter({
        "service_url": "http://10.0.0.18:60008/",
        "username": "root",
        "password": "changeme",
        "bridge": "spnode1",
        "stage_dir": "/root/.stage",
        "run_dir": "hdfs:///etl/runs",
    })
    job_submitter.run("hdfs://etl/apps/myapp/1.0.0.1", options={
        "conf": {
            'spark.yarn.appMasterEnv.PYSPARK_PYTHON': 'python3',
            'spark.executorEnv.PYSPARK_PYTHON': 'python3'
        }
    })
```
In the example above:
* It submit the job to the livy interface at `http://10.0.0.18:60008/`
* Assuming the livy username and password is `root` and `changeme`, for anonymous access, you can omit username and password
* It will create a `run_directory` at `hdfs:///etl/runs/<run_uuid>`, the run_uuid will be passed to the job, this directory is for we to store the job input (aka the args parameter you pass to the run function)
* When you run the job, you pass some spark config to tell the job to use python3
* In this example, when you run the job, args is default (which is an empty dict). You can pass args in optional parameter args
* (TODO) Once the job is done, you will receive the yarn log for the job.

