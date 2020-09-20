from pyspark.sql import SparkSession, SQLContext, Row

def main(spark, input_args):
    Student = Row("id", "name")
    df = spark.createDataFrame([
        Student(1, 'Liu Bei'),
        Student(3, 'Guan Yu'),
        Student(2, 'Zhang Fei')
    ])
    df.show()

