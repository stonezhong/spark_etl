# Test with Your Spark Cluster

Assumptions:
* You have a Spark Cluster installed
* You have a bridge host setup
    * You can `ssh` to the bridge host without the need to enter password, you can config your `~/.ssh/config` to get this done.
    * The bridge host can access HDFS via `hdfs` command.
* Your Spark Cluster has a livy interface for user to submit jobs.


This example shows how you can build, deploy and run sample application in myapp directory. You can create your own data application in separate directroy and follow the same steps.

<details>
<summary>step 1: use the config file <code>config.json</code></summary>
<br />

* It uses the config in `config.json`, you can modify it if needed.
* config for stage host
    * Make sure `deployer.args[0].bridge` points to the bridge host name or ip
    * Make sure you have directory `deployer.args[0].stage_dir` created on bridge host
* Config your livy
    * `job_submitter.args[0].service_url` must point to the livy endpoint
    * `job_submitter.args[0].username` specify your livy user's username
    * `job_submitter.args[0].password` specify your livy user's password
    * `job_submitter.args[0].run_dir` points to the run_dir in your HDFS

Now you need to create 2 directories in hdfs, you can do it in bridge host:
```
hdfs dfs -mkdir -p hdfs:///etl/runs
hdfs dfs -mkdir -p hdfs:///etl/apps
```

Note, setup of spark cluster is not covered by this document.
</details>


<details>
<summary>step 2: build application</summary>
<br />

do this:
```
./etl.py -a build --app-dir ./myapp --build-dir ./myapp/build
```
</details>

<details>
<summary>step 3: deploy application</summary>
<br />

do this:
```
./etl.py -a deploy \
    -c config.json \
    --build-dir ./myapp/build \
    --deploy-dir hdfs:///etl/apps/myapp
```
</details>

<details>
<summary>step 4: run application</summary>
<br />

do this:
```
./etl.py -a run \
    -c config.json \
    --deploy-dir hdfs:///etl/apps/myapp \
    --version 1.0.0.1 \
    --args foo.json
```

***To check the application log, you can go to the bridge host and run: yarn logs -applicationId <application_id>***
</details>

