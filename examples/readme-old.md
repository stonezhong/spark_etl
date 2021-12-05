# Index
* [Build sample app](#build-sample-app)
* [Example 1: using spark cluster at home](#example-1-using-spark-cluster-at-home)
* [Example 2: using spark cluster in AWS EMR](#example-2-using-spark-cluster-in-aws-emr)
* [Example 3: using local pyspark](#example-3-using-local-pyspark)
* [Example 4: using OCI DataFlow via instance principle](#example-4-using-oci-dataFlow-via-instance-principle)


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

# Example 4: using OCI DataFlow via instance principle
```bash
# rebuild application
# Since for OCI, you need to import oci and oci-core, so do the following
# uncomment oci, oci-core in common_requirements.txt and rebuild
./etl.py \
    -a build \
    --app-dir ./myapp \
    --build-dir .builds/


# generate config
./gen_cfg.py \
    --deployer DataflowDeployer \
    --submitter DataflowJobSubmitter \
    --oci-region            IAD \
    --oci-compartment-id ocid1.compartment.oc1..aaaaaaaaxq5efno25dndoaltcitvtprfelpui3al3nul6bdmihc6sggbho5a \
    --oci-driver-shape      VM.Standard2.1 \
    --oci-executor-shape    VM.Standard2.1 \
    --oci-num-executors     1 \
    --run-dir               oci://dataflow-runs@idrnu3akjpv5/beta_hwd \
> config.json

# deploy
# Note, you nee to install oci to deal with oci
pip install oci oci-core

./etl.py \
    -a deploy \
    -c config.json \
    --build-dir .builds/ \
    --deploy-dir oci://dataflow-apps@idrnu3akjpv5/beta_hwd/myapp

# run
./etl.py \
    -a run \
    -c config.json \
    --deploy-dir oci://dataflow-apps@idrnu3akjpv5/beta_hwd/myapp \
    --version 1.0.0.1
```
