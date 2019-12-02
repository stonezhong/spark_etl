from .base import Namespace
from ..core import DataDescriptor

class SQLDataDescriptor(DataDescriptor):
    def __init__(self, namespace):
        super(SQLDataDescriptor, self).__init__(namespace)


class SQLTableDataDescriptor(SQLDataDescriptor):
    def __init__(self, namespace, db_name, table_name):
        super(SQLTableDataDescriptor, self).__init__(namespace)
        self.db_name = db_name
        self.table_name = table_name


class SQLQueryDataDescriptor(SQLDataDescriptor):
    def __init__(self, namespace, query_name, query):
        super(SQLQueryDataDescriptor, self).__init__(namespace)
        self.query_name = query_name
        self.query = query


class SQLNamespace(Namespace):
    def __init__(self, name, config={}):
        super(SQLNamespace, self).__init__(name)
        self.config=config
    
    def create_data_descriptor(self, **kwargs):
        if kwargs['type']=='table':
            return SQLTableDataDescriptor(self, kwargs['db_name'], kwargs['table_name'])

        if kwargs['type']=='query':
            return SQLQueryDataDescriptor(self, kwargs['query_name'], kwargs['query'])

