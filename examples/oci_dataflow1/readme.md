# Demo hightlights
* How to build data application
* How to deploy data application
* How to run data application in pyspark
    * The app create a data frame
    * The app save data frame to hdfs
    * The app load data frame from hdfs
    * The app transforms data frame using SparkSQL

# Before the experiment
Please setup virtual environment first, see [readme.md](../readme.md).
also make sure you have JRE 1.8 installed

Make sure you have your ssh config correct in `.artifacts` directory

# Build app
```bash
etl -a build -c config.json -p demo01
```
* This command build the application
* The application name is `demo01` located at directory `apps/demo01`. 
* Build result will be in `.builds/demo01`


# To deploy
```bash
etl -a deploy -c config.json -p demo01 -f main
```
* This command deploy the application `demo01`
* The application `demo01` is deployed to hdfs at `hdfs://spnode1:9000//etl/apps/demo01/1.0.0.0`

# To run
```bash
etl -a run -c config.json -p demo01 -f main --run-args input.json
```
* This command run the application `demo01`, using profile `.profiles/main.json`
* It passes the content of `input.json` as parameter to the app
* You can see files in your HDFS.
