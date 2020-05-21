from .base import Driver
import pandas as pd
import numpy as np
from pandasql import sqldf
import pandas.io.sql as psql

from ..namespaces.lfs import LFSDataDescriptor
from ..namespaces.sql import SQLDataDescriptor, SQLQueryDataDescriptor, SQLTableDataDescriptor
from ..core import DataObject

class PandasDataObject(DataObject):
    def __init__(self, driver, data_descriptor=None):
        super(PandasDataObject, self).__init__(driver, data_descriptor)
        self.df = None
    
    def load(self):
        if isinstance(self.data_descriptor, LFSDataDescriptor):
            if self.data_descriptor.format=='csv':
                self.df = pd.read_csv(self.data_descriptor.full_path)
                return
            raise Exception("Unsupported format")
        if isinstance(self.data_descriptor, SQLDataDescriptor):
            namespace = self.data_descriptor.namespace
            con = None
            if namespace.config['type']=='mysql':
                import mysql.connector
                con = mysql.connector.connect(
                    user=namespace.config['user'],
                    password=namespace.config['password'],
                    host=namespace.config['host'],
                    database=namespace.config['database'],
                    port=namespace.config.get('port', 3306)
                )
            if con is None:
                raise Exception("Unsupported database type")
            query = None
            if isinstance(self.data_descriptor, SQLTableDataDescriptor):
                query = 'SELECT * FROM {}.{}'.format(self.data_descriptor.db_name, self.data_descriptor.table_name)
            else:
                query = self.data_descriptor.query
            self.df = psql.read_sql(query, con=con)
            return
            
        raise Exception("Unsupported namespace")

    

class PandasDriver(Driver):
    def __init__(self):
        super(PandasDriver, self).__init__(config=None)
    
    def connect(self):
        pass

    def copy_to(self, do_from, do_to):
        raise NotImplementedError()

    def append_to(self, do_from, do_to):
        raise NotImplementedError()


    def get_data_object(self, data_descriptor):
        if isinstance(data_descriptor, LFSDataDescriptor):
            return PandasDataObject(self, data_descriptor=data_descriptor)
        if isinstance(data_descriptor, SQLDataDescriptor):
            return PandasDataObject(self, data_descriptor=data_descriptor)
        raise Exception("Unsupported namespace")

