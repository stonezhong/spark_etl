from pyspark.sql import SparkSession, SQLContext, Row
# from spark_etl.utils import cli_main

def main(spark, input_args, sysops={}):
    # return cli_main(spark, input_args, sysops)
    print("=====================")
    print(input_args)
    print("=====================")

    Student = Row("id", "name")
    df = spark.createDataFrame([
        Student(1, 'Apple'),
        Student(3, 'Orange'),
        Student(2, 'Banana')
    ])
    df.show()

    # df = spark.read.json("s3://stonezhong-lakehouse/data/tests/test_data.json")
    # df.show()
    return {"result": "ok"}

