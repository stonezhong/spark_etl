from .sql import SQLDriver
import mysql.connector


class MySQLDriver(SQLDriver):
    def __init__(self, config=None):
        super(MySQLDriver, self).__init__(config=config)
    
    def connect(self):
        self.cnx = mysql.connector.connect(
            user     = self.config["user"], 
            password = self.config["password"],
            host     = self.config["host"],
            database = self.config["database"],
            port     = self.config['port']
        )

    def has_table(self, do):
        try:
            cursor = self.cnx.cursor()
            to_execute = "SELECT 1 FROM {}.{}".format(
                do.db_name, do.table_name, 
            )
            cursor.execute(to_execute)
            self.cnx.rollback()
            return True
        except mysql.connector.ProgrammingError as e:
            self.cnx.rollback()
            if e.errno==1146:
                return False
            raise


    def copy_to(self, from_do, to_do):
        try:
            cursor = self.cnx.cursor()
            to_execute = "CREATE TABLE {}.{} AS ({})".format(
                to_do.db_name, to_do.table_name, 
                from_do.query
            )
            cursor.execute(to_execute)
            self.cnx.commit()
        except:
            self.cnx.rollback()
            raise

    def append_to(self, from_do, to_do):
        if not self.has_table(to_do):
            self.copy_to(from_do, to_do)
            return

        try:
            cursor = self.cnx.cursor()
            to_execute = "INSERT INTO {}.{} ({})".format(
                to_do.db_name, to_do.table_name, 
                from_do.query
            )
            cursor.execute(to_execute)
            self.cnx.commit()
        except:
            self.cnx.rollback()
            raise
