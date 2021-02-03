from pyspark.sql import SparkSession, SQLContext, Row

def main(spark, input_args, sysops={}):
    print("=====================")
    print(input_args)
    print("=====================")

    Student = Row("id", "name")
    df = spark.createDataFrame([
        Student(1, 'Liu Bei'),
        Student(3, 'Guan Yu'),
        Student(2, 'Zhang Fei')
    ])
    df.show()
    return {"result": "ok"}

