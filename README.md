# Deployers
## HDFSDeployer
**Deploy application build to HDFS via a bridge host**

To create a deployer, here is the sample code:
* `bridge` is a ssh hostname where you can run the `hdfs dfs ...` command
* `stage_dir` is a temporary directory in `bridge` machine, for storing temporary files.
```
    deployer = HDFSDeployer({
        "bridge"   : "spnode1",
        "stage_dir": "/root/.stage_dir",
    })
```

To deploy an application, here is the sample code:
* Frist parameter tells where is the application `build`. You need to build to this directory first
* Second parameter tell where is the destination to deploy the application.
```
    deployer.deploy(
        "/mnt/DATA_DISK/projects/spark_etl/examples/myapp/build", 
        "/apps/myjob"
    )
```

# Job Submitters
## LivyJobSubmitters
To create a job submitter, here is the sample code:
* `service_url` points to the livy endpoint
* `username`, `password` is your livy username and password
* `bridge`: is a ssh hostname, where you can run `yarn logs -applicationId` to get the application log

Here is an example:
```
    job_submitter = LivyJobSubmitter({
        "service_url": "http://10.0.0.11:60008/",
        "username": "root",
        "password": "foo",
        "bridge": "spnode1"
    })
```

To run the application, here is the sample:
* first parameter is the deployment location. The deployer is responsible for the deployment.
* `/apps/myjob/build/1.0.0.1` resides  in HDFS
```
    job_submitter.run(
        "/apps/myjob/build/1.0.0.1"
    )
```