# Build sample app
```bash
./etl.py \
    -a build \
    --app-dir ./myapp \
    --build-dir .builds/
```

# Examples 1: using spark cluster at home
```bash
# Test at home, Generate config
./gen_cfg.py \
    --deployer HDFSDeployer \
    --submitter LivyJobSubmitter \
    --livy-host spnode1 \
    --livy-via-tunnel \
    --bridge spnode1 \
    --stage-dir /root/.stage \
    --run-dir hdfs:///demo-etl/runs \
    --ssh-config ssh_configs/home/config \
    --ssh-key home=/mnt/DATA_DISK/ssh/keys/local/home > config.json

# deploy the job
./etl.py \
    -a deploy \
    -c config.json \
    --build-dir .builds/ \
    --deploy-dir hdfs:///demo-etl/apps/myapp


# run the job
./etl.py \
    -a run \
    -c config.json \
    --deploy-dir hdfs:///demo-etl/apps/myapp \
    --version 1.0.0.1
```

# Example 2: using spark cluster in AWS EMR
```bash
# Test at AWS, Generate config
./gen_cfg.py \
    --deployer S3Deployer \
    --submitter LivyJobSubmitter \
    --aws-account ~/.aws/account.json \
    --livy-host 127.0.0.1 \
    --livy-via-tunnel \
    --bridge emr \
    --stage-dir /home/hadoop/.stage \
    --run-dir s3://stonezhong-lakehouse/runs \
    --ssh-config ssh_configs/aws-emr/config \
    --ssh-key for-emr=/mnt/DATA_DISK/ssh/keys//aws/for-emr.pem > config.json

# deploy the job
./etl.py \
    -a deploy \
    -c config.json \
    --build-dir .builds/ \
    --deploy-dir s3://stonezhong-lakehouse/apps/myapp

# run the job
./etl.py \
    -a run \
    -c config.json \
    --deploy-dir s3://stonezhong-lakehouse/apps/myapp \
    --version 1.0.0.1
```

# Example 3: using local pyspark
```bash
# application launched locally
# run-dir also is local
# can access S3 bucket for loading data
./gen_cfg.py \
    --deployer LocalDeployer \
    --submitter PySparkJobSubmitter \
    --aws-account ~/.aws/account.json \
    --enable-aws-s3 \
    --run-dir /mnt/DATA_DISK/test-datalake/runs > config.json

# deploy the job
./etl.py \
    -a deploy \
    -c config.json \
    --build-dir .builds/ \
    --deploy-dir /mnt/DATA_DISK/test-datalake/apps/myapp

# run the job
./etl.py \
    -a run \
    -c config.json \
    --deploy-dir /mnt/DATA_DISK/test-datalake/apps/myapp \
    --version 1.0.0.1
```
