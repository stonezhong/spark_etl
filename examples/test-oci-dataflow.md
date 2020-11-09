# Test with Your Spark Cluster

Assumptions:
* You have a tenany in OCI, have Dataflow service enabled

This example shows how you can build, deploy and run sample application in myapp directory. You can create your own data application in separate directroy and follow the same steps.

<details>
<summary>step 1: use the config file <code>config_oci.json</code></summary>
<br />

* It uses the config in `config_oci.json`, you can modify it if needed.
* `deployer.args[0].region`: specify the region your Dataflow app will be created
* `deployer.args[0].oci_config`: specify your oci configuration.
* `deployer.args[0].dataflow`: specify the dataflow related config
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
    -c config_oci.json \
    --build-dir ./myapp/build \
    --deploy-dir oci://dataflow-apps@idrnu3akjpv5/testapps/myapp
```
* `oci://dataflow-apps@idrnu3akjpv5/testapps/myapp` is the OCI Object Storage URL
    * `dataflow-apps` is a bucket name, you must create this bucket
    * `idrnu3akjpv5` is the namespace of your tenancy
    * `/testapps/myapp` is the path of where your build will be placed
</details>

<details>
<summary>step 4: run application</summary>
<br />

do this:
```
./etl.py -a run \
    -c config_oci.json \
    --deploy-dir oci://dataflow-apps@idrnu3akjpv5/testapps/myapp \
    --version 1.0.0.1 \
    --args foo.json
```
</details>

