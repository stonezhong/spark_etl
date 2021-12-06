# In this demo, you will see
* How to build data application
* How to deploy data application on your dev machine using pyspark
* How to run data application in pyspark
    * The application create a data frame
    * The application save data frame to local filesystem
    * The application load data frame from local filesystem
    * The application transforms data frame using SparkSQL
    
# Before the experiment
* Please setup virtual environment first, see [readme.md](../readme.md).
* Make sure you have JRE installed. Here is an example on installing JRE on RedHat Linux.
```bash
# On RHEL 7
sudo yum install java-1.8.0-openjdk
```
* Install package `pyspark`
```bash
python -m pip install pyspark
```

# Build app
```bash
etl -a build -p demo01
```
* It build the application `demo01`
* It uses configuration file `config.json`
* Since `apps_dir=apps` in configuration, it will locate application `demo01` at direcotry `apps/demo01`
* Since `builds_dir=.builds` in configuration, build result will be in `.builds/demo01`


# To deploy
```bash
etl -a deploy -p demo01 -f main
```
* It deploy the application `demo01`
* It uses profile `main`
* Since `profiles_dir=.profiles` in configuration, it will load profile `main` from file `.profiles/main.json`
* It will deploy to directory `.deployments/demo01/1.0.0.0`, since `deploy_base=.deployments` in profile `main`, and application version is `1.0.0.0` from it's manifest file.

# To run
```bash
etl -a run -p demo01 -f main --run-args input.json
```
* It run the application `demo01`, using profile `main`
* It passes the content of `input.json` as parameter to the app
* based on the cmds in `input.json`, it will save parquet to `.data/trade.parquet`.
