# Demo hightlights
* How to build data application
* How to deploy data application
* How to run data application in pyspark
    * The app create a data frame
    * The app save data frame to aws s3 bucket
    * The app load data frame from aws s3 bucket
    * The app transforms data frame using SparkSQL

# Before the experiment
Please setup virtual environment first, see [readme.md](../readme.md).
also make sure you have JRE 1.8 installed

Make sure you have your ssh config correct in `.artifacts` directory

## Update your HDFS cluster to support AWS S3
First, check your hadoop version
```bash
hdfs version
```
My version is 3.1.2, so
* I will download `hadoop-aws-3.1.2`
* I will also download `aws-java-sdk-bundle-1.11.271.jar` since it is required by `hadoop-aws-3.1.2`
* `hadoop-aws` version must match your HDFS version. [see also](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.1.2)

Now do it on all hdfs nodes:
```
vi $HADOOP_HOME//etc/hadoop/core-site.xml
adding following sections: (you need to change *** to the right value)

  <property>
    <name>fs.s3a.access.key</name>
    <value>***</value>
   </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>***</value>
  </property>
```

```bash
cd $HADOOP_HOME/share/hadoop/common/lib
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.2/hadoop-aws-3.1.2.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar
```

Once you are done, restart the HDFS cluster (on namenode)
```bash
stop-dfs.sh
start-dfs.sh
```
Now you should be able to do the following on hdfs node:
```bash
hdfs dfs -ls s3a://spark-etl-demo/
```

Now, copy following jars to HDFS, so you can use s3a:// in spark job as well:
```
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar
hdfs dfs -put aws-java-sdk-1.7.4.jar /spark_jars/aws-java-sdk-1.7.4.jar
hdfs dfs -put hadoop-aws-2.7.3.jar /spark_jars/hadoop-aws-2.7.3.jar
```

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
