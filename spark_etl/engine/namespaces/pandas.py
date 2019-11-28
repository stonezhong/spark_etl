import json
from .base import Namespace
from ..core import DataObject

# class SQLDataObject(DataObject):
#     def __init__(self, namespace, path, format):
#         super(SQLDataObject, self).__init__(namespace, path, format)
#         p1 = path.find('/')
#         p2 = path.find('/', p1+1)
#         if p1 == -1 or p2 == -1:
#             raise Exception('bad path: {}'.format(path))
#         self.db_name = path[p1+1, p2]
#         self.view_name = path[p2+1]

    
#     def to_df(self, **kwargs):
#         return self.namespace.to_df(self.path, self.format, **kwargs)

class PandasNamespace(Namespace):
    def __init__(self, name, connection_info):
        super(SQLNamespace, self).__init__(name)
        self.connection_info = connection_info

        if self.connection_info['type'] == 'mysql':
            import mysql.connector
            conn = mysql.connector.connect(
                user=connection_info['user'],
                password=connection_info['password'],
                host=connection_info['host'],
                database=connection_info['database']
            )
        else:
            raise Exception('Unrecognized DB type')
        

    def get_data_object(self, path, format):
        return SQLDataObject(self, path, format)


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
