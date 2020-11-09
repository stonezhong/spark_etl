# Test with Local Spark (pyspark package)

You can use this setup if you want to test your data application with small amount of data on your desktop or laptop.

This example shows how you can build, deploy and run sample application in myapp directory. You can create your own data application in separate directroy and follow the same steps.

<details>
<summary>step 1: use the config file <code>config_local.json</code></summary>
<br />

* It uses the config in [`config_local.json`](config_local.json), you can modify it if needed.

Make bunch of directories:
```
mkdir -p $HOME/etl_lab/runs
mkdir -p $HOME/etl_lab/apps
```

You also need to install pyspark:
```
pip install pyspark
```

* And you need to install Java 1.8 if you do not have it.
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
./etl.py -a deploy --build-dir ./myapp/build --deploy-dir $HOME/etl_lab/apps/myapp -c config_local.json
```
</details>

<details>
<summary>step 4: run application</summary>
<br />

do this:
```
./etl.py -a run --deploy-dir $HOME/etl_lab/apps/myapp --version 1.0.0.1 -c config_local.json
```

</details>

