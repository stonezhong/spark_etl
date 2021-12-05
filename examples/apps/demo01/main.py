from pyspark.sql import SparkSession, SQLContext, Row

def main(spark, input_args, sysops={}):
    print("=====================")
    print(input_args)
    print("=====================")

    df = None
    for cmd in input_args['cmds']:
        action = cmd['action']
        if action == "create_trade":
            Trade = Row("type", "symbol", "price", "amount", "commission")
            df = spark.createDataFrame([
                Trade("BUY",  "AAPL", 200.0, 20, 8.1),
                Trade("BUY",  "AMD",  140.0, 10, 8.2),
                Trade("SELL", "ORCL", 93.0,  40, 8.15),
            ])
            df.show()
        elif action == "save_trade":
            df.write.mode('overwrite').parquet(cmd['location']) 
        elif action == "load_trade":
            spark.read.parquet(cmd['location'])
        elif action == "cash_flow":
            df.createOrReplaceTempView("trade")
            spark.sql("SELECT price*amount*IF(type=='SELL', 1, -1)-commission as net_cash FROM trade").show()

    return {"result": "ok"}

