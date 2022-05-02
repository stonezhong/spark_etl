# Goal
* Show a demo that build, deploy and run your spark application with on AWS EMR spark cluster, with application deployed to AWS S3

# Create spark cluster in AWS EMR

[Here is an example](setup-emr.md), make sure you write down the master node IP address once the clsuter is started.

# Build app
```bash
etl -a build -p demo01
```
* It build the application `demo01`
* The config file is `config.json` unless specified by -c option
* Since `apps_dir=apps` in config, it will locate application `demo01` at direcotry `apps/demo01`
* Since `builds_dir=.builds` in configuration, build result will be in `.builds/demo01`

# Deploy app
```bash
etl -a deploy -p demo01 -f main
```
* This command deploy the application `demo01`
* It uses profile `main`
* Since `profiles_dir=.profiles` in `config.json`, it will load profile `main` from file `.profiles/main.json`
* It will deploy to directory `s3://spark-etl-demo/apps/demo01/1.0.0.0`, since `deploy_base` in profile `main`, and application version is `1.0.0.0` from it's manifest file.

# Run app
```bash
etl -a run -p demo01 -f main --run-args input.json
```
* It run the application `demo01`, using profile `main`
* It passes the content of `input.json` as parameter to the data application
* based on the cmds in `input.json`, it will save parquet to `s3a://spark-etl-demo/data/trade.parquet`.
* The application returns a dict `{"result": "ok"}`

