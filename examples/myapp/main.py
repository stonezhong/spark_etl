from pyspark.sql import SparkSession, SQLContext, Row

def main(spark, input_args, sysops={}):
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
    return {"result": "ok"}

