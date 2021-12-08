# You will see following demos here
* Build a data application
* Deploy a data application to AWS S3 bucket
* Run the data application in AWS EMR
    * The app create a data frame
    * The app save data frame to aws s3 bucket
    * The app load data frame from aws s3 bucket
    * The app transforms data frame using SparkSQL

# Create spark cluster in AWS EMR

[Here is an example](setup-emr.md), make sure you write down the master node IP address once the clsuter is started.

# Build app
```bash
etl -a build -p demo01
```
* This command build the application
* The application name is `demo01` located at directory `apps/demo01`. 
* Build result will be in `.builds/demo01`


# To deploy
```bash
etl -a deploy -p demo01 -f main
```
* This command deploy the application `demo01`
* The application `demo01` is deployed to hdfs at `hdfs://spnode1:9000//etl/apps/demo01/1.0.0.0`

# To run
```bash
etl -a run -p demo01 -f main --run-args input.json
```
* This command run the application `demo01`, using profile `.profiles/main.json`
* It passes the content of `input.json` as parameter to the app
* You can see files in your runs directory.

