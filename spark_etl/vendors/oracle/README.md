# Deployers
## DataflowDeployer
**Deploy application build to OCI DataFlow Service**

To create a deployer, here is the sample code:
* `region`: the region of the OCI you are using.
* `dataflow.compartment_id`: in which compartment the DataFlow Application will be created.
* `dataflow.driver_shape`: the driver's shape
* `executor_shape`: the executor's shape
* `num_executors`: the number executor you needed
```
deployer = DataflowDeployer({
    "region": "IAD",
    "dataflow": {
        "compartment_id": "XYZ",
        "driver_shape": "VM.Standard2.1",
        "executor_shape": "VM.Standard2.1",
        "num_executors": 1
    }
})
```

To deploy an application, here is the sample code:
* Frist parameter tells where is the application `build`. You need to build to this directory first
* Second parameter tell where is the destination to deploy the application.
```
    deployer.deploy(
        "/mnt/DATA_DISK/projects/spark_etl/examples/myapp/build", 
        "oci://dataflow-apps@idrnu3akjpv5/hwd/myjob"
    )
```

# Job Submitters
## DataflowJobSubmitter
This job job submitter will submit a deployed application to OCI DataFlow Service. Here is the sample code:
* `region`: The OCI region for your DataFlow Applicaiton.

Here is an example:
```
job_submitter = DataflowJobSubmitter({
    "region": 'IAD',
})
```

To run the application, here is the sample:
* first parameter is the deployment location. The deployer is responsible for the deployment.
* `options.display_name`: It is the name of the run
```
job_submitter.run(
    "oci://dataflow-apps@idrnu3akjpv5/hwd/myjob/1.0.0.1", {
        "display_name": "test spark_etl"
    }
)
```
