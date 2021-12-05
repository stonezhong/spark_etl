# Demo hightlights
* How to build data application
* How to deploy data application
* How to run data application in pyspark
    * The app create a data frame
    * The app save data frame to aws s3 buckets
    * The app load data frame from aws s3 buckets
    * The app transforms data frame using SparkSQL

# Before the experiment
Please setup virtual environment first, see [readme.md](../readme.md).
also make sure you have JRE 1.8 installed
```bash
# On RHEL 7
sudo yum install java-1.8.0-openjdk
```

Then you need to install package pyspark
```bash
python -m pip install pyspark
```

Make sure you have your aws credential in ~/.aws/account.json, the file looks like below, which has your aws access key id and secret key.
```json
{
    "aws_access_key_id": "XXX",
    "aws_secret_access_key": "YYY"
}

```
Create a aws s3 bucket: assuming your bucket name is `spark-etl-demo`

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
* The application `demo01` is deployed to s3 at `s3://spark-etl-demo/apps/demo01/1.0.0.0`

# To run
```bash
etl -a run -c config.json -p demo01 -f main --run-args input.json
```
* This command run the application `demo01`, using profile `.profiles/main.json`
* It passes the content of `input.json` as parameter to the app
* You can see files in your S3 bucket.
