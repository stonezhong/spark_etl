# Goal
* Show a demo that build, deploy and run your spark application with Oracle OCI Dataflow spark cluster, with application deployed to OCI bucket

# Before the experiment

<details>
<summary>Setup Python Virtual Environment</summary>

```bash
mkdir .venv
python3 -m venv .venv
source .venv/bin/activate
python -m pip install pip setuptools --upgrade
python -m pip install wheel
python -m pip install spark-etl
python -m pip install oci-core
```
</details>

<details>
<summary>check out demos</summary>

```bash
git clone https://github.com/stonezhong/spark_etl.git
cd spark_etl/examples/oci_dataflow2
```
</details>

# Build app
```bash
etl -a build -p demo01
```
* It build the application `demo01`
* The config file is `config.json` unless specified by -c option
* Since `apps_dir=apps` in config, it will locate application `demo01` at direcotry `apps/demo01`
* Since `builds_dir=.builds` in configuration, build result will be in `.builds/demo01`

# Config OCI with API key
<details>
<summary>Setup OCI Config</summary>

You can change the config in profile `.profiles/main.json` if needed.

* You should setup instance principal, see [setup instance principal](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm) for details
</details>

# Deploy app
```bash
etl -a deploy -p demo01 -f main
```
* This command deploy the application `demo01`
* It uses profile `main`
* Since `profiles_dir=.profiles` in `config.json`, it will load profile `main` from file `.profiles/main.json`
* It will deploy to directory `oci://dataflow-apps@idrnu3akjpv5/spark-etl-lab/apps/demo01/1.0.0.0`, since `deploy_base` in profile `main`, and application version is `1.0.0.0` from it's manifest file.

# Run app
```bash
etl -a run -p demo01 -f main --run-args input.json
```
* It run the application `demo01`, using profile `main`
* It passes the content of `input.json` as parameter to the data application
* based on the cmds in `input.json`, it will save parquet to `oci://spark-etl-lab@idrnu3akjpv5/data/trade.parquet`.
* The application returns a dict `{"result": "ok"}`
