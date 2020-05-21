import json
from .base import Namespace
from ..core import DataObject

class JDBCDataObject(DataObject):
    def __init__(self, namespace, path, format):
        super(JDBCDataObject, self).__init__(namespace, path, format)
    
    def to_df(self, **kwargs):
        return self.namespace.to_df(self.path, self.format, **kwargs)

class JDBCNamespace(Namespace):
    def __init__(self, name, spark, username, password, url):
        super(JDBCNamespace, self).__init__(name)
        self.spark = spark
        self.username = username
        self.password = password
        self.url = url
        self.query_defs = {} 


    def register_query(self, name, body):
        self.query_defs[name] = body


    def get_data_object(self, path, format):
        return JDBCDataObject(self, path, format)


    def load_table(self, query_name, query_args):
        query = self.query_defs[query_name].format(**query_args)
        table = "({}) tmp".format(query)

        df = self.spark.read \
            .format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", table) \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
        for column in df.columns:
            df = df.withColumnRenamed(column, column.lower())
        return df


    def to_df(self, path, format, **kwargs):
        if format == 'query':
            p1 = path.find('/')
            p2 = path.find('/', p1+1)

            if p2 == -1:
                query_name = path[p1+1:]
                query_args = {}
            else:
                query_name = path[p1+1:p2]
                query_args = json.loads(path[p2+1:])
            return self.load_table(query_name, query_args)

        raise ValueError("format {} is not supported".format(format))
