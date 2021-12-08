# You will see following demos here
* Build a data application
* Deploy a data application to AWS S3 bucket
* Run the data application in Spark Cluster
    * The spark job is submitted via a livy service
    * The app create a data frame
    * The app save data frame to aws s3 bucket
    * The app load data frame from aws s3 bucket
    * The app transforms data frame using SparkSQL

# Before the experiment
Please setup virtual environment first, see [readme.md](../readme.md).
also make sure you have JRE 1.8 installed

Make sure you have your ssh config correct in `.artifacts` directory

## Update your HDFS cluster to support AWS S3
Since I have hadoop 2.7.3. So I need to copy following 3 files to all spark nodes at `$SPARK_HOME/jars`

* `hadoop-aws-2.7.3.jar`
* `aws-java-sdk-1.7.4.jar`
* `jets3t-0.9.0.jar`

Then change `$HADOOP_HOME/etc/hadoop/core-site.xml` on all nodes:
adding following sections: (you need to change *** to the right value)
```
  <property>
    <name>fs.s3a.access.key</name>
    <value>***</value>
   </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>***</value>
  </property>
```

Once you are done, restart the HDFS cluster (on namenode)
```bash
stop-dfs.sh
start-dfs.sh
```

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
* The application `demo01` is deployed to AWS S3 bucket at `s3://spark-etl-demo/apps/demo01/1.0.0.0`

# To run
```bash
etl -a run -p demo01 -f main --run-args input.json
```
* This command run the application `demo01`, using profile `.profiles/main.json`
* It passes the content of `input.json` as parameter to the app
* You can see files in AWS S3 bucket.
