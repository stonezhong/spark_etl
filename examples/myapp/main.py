from pyspark.sql import SparkSession, SQLContext, Row

spark = SparkSession.builder.appName("RunJob").getOrCreate()

Student = Row("id", "name")
df = spark.createDataFrame([
    Student(1, 'Liu Bei'),
    Student(3, 'Guan Yu'),
    Student(2, 'Zhang Fei')
])
df.show()

