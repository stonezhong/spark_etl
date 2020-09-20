# Overview
## Goal
This is a cross platform tool allowing you to build, deploy and run your ETL job. It supports native Apache Spark cluster, Amazon EMR and Oracle DataFlow, which means:
* You can use this library if you build your own Apache Spark cluster
* You can use this library if you use Amazon EMR
* You can use this library if you use Oracle DataFlow

# Application

## What is an application?
An application is the code for a spark job. It contains:
* A `main.py` file which contain the application entry
* A `manifest.json` file, which specify the metadata of the application, for example, the current version, check [here](examples/myapp/manifest.json) for example.
* A `requirements.txt` file, which specify the application dependency.

## How to build an application
Building an application will generate application artifacts which is needed when you deploy the application.

Here is sample code to build application:
```
from spark_etl import Application
...

app = Application("path_to_application_dir")
app.build("path_do_artifact_directory")

# it load the application from path_to_application_dir
# it generate artifacts in path_do_artifact_directory
```

## Application entry signature
In your application's `main.py`, you shuold have a function called `main` with the following signature:
```
def main(spark, input_args):
    # your code here
```

* The argument spark is the spark session object passed to you
* The argument input_args is a dict that represent the arguments when you invoke the application, by default it is an empty dict

See [here](examples/myapp/main.py) for example.

# What is deployer
A deployer is an object that knows how to deploy your ETL job in a paticular platform

# What is submitter
A submitter is an object that knows how to submit your ETL job in a paticular platform.

# [Deploy and Submit job with native Apache Spark](vendor-native.md)
# Deploy and Submit job with Oracle DataFlow

