from .base import Namespace
from ..core import DataObject

class SQLDataObject(DataObject):
    def __init__(self, namespace, path):
        super(SQLDataObject, self).__init__(namespace, path)


class SQLTableDataObject(SQLDataObject):
    def __init__(self, namespace, path, db_name, table_name):
        super(SQLTableDataObject, self).__init__(namespace, path)
        self.db_name = db_name
        self.table_name = table_name


class SQLQueryDataObject(SQLDataObject):
    def __init__(self, namespace, path, query_name, query):
        super(SQLQueryDataObject, self).__init__(namespace, path)
        self.query_name = query_name
        self.query = query


class SQLNamespace(Namespace):
    def __init__(self, name):
        super(SQLNamespace, self).__init__(name)
        self.cache = {}
    
    def create_do(self, path, **kwargs):
        if kwargs['type']=='table':
            do = SQLTableDataObject(self, path, kwargs['db_name'], kwargs['table_name'])
            self.cache[path] = do
            return do

        if kwargs['type']=='query':
            do = SQLQueryDataObject(self, path, kwargs['query_name'], kwargs['query'])
            self.cache[path] = do
            return do

    def do(self, path):
        return self.cache[path]
