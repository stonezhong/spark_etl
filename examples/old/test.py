from pyspark.sql import Row
Student = Row("id", "name")
students = spark.createDataFrame([
    Student(1, 'Liu Bei'),
    Student(3, 'Guan Yu'),
    Student(2, 'Zhang Fei')
])

