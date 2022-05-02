# Goal
* Show a demo that build, deploy and run your spark application with pyspark

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
```
</details>

<details>
<summary>Install JRE</summary>

You can skip this if JRE is already installed.
```bash
sudo yum install java-1.8.0-openjdk
```
</details>

<details>
<summary>Install pyspark</summary>

```bash
python -m pip install pyspark
```
</details>

<details>
<summary>Install boto3</summary>

This is the aws client package.
```bash
python -m pip install boto3
```
</details>

<details>
<summary>download Hadoop AWS Support libraries</summary>

```bash
wget \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar \
    -P `python3 -c "import site;print([p for p in site.getsitepackages() if p.endswith(('site-packages', 'dist-packages')) ][0])"`/pyspark/jars

wget \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar \
    -P `python3 -c "import site;print([p for p in site.getsitepackages() if p.endswith(('site-packages', 'dist-packages')) ][0])"`/pyspark/jars
```
</details>

<details>
<summary>Prepare your AWS credential</summary>

Make sure you have your aws credential in ~/.aws/account.json, the file looks like below, which has your aws access key id and secret key.
```json
{
    "aws_access_key_id": "XXX",
    "aws_secret_access_key": "YYY"
}
```
</details>

<details>
<summary>Create a S3 bucket for the demo</summary>

Create a aws s3 bucket: assuming your bucket name is `spark-etl-demo`
</details>


<details>
<summary>check out demos</summary>

```bash
git clone https://github.com/stonezhong/spark_etl.git
cd spark_etl/examples/pyspark_s3
```
</details>

# Build app
```bash
etl -a build -p demo01
```
* This command build the application
* The application name is `demo01` located at directory `apps/demo01`. 
* Build result will be in `.builds/demo01`


# Deploy app
```bash
etl -a deploy -p demo01 -f main
```
* This command deploy the application `demo01`
* The application `demo01` is deployed to s3 at `s3://spark-etl-demo/apps/demo01/1.0.0.0`

# Run app
```bash
etl -a run -p demo01 -f main --run-args input.json
```
