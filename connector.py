import pyodbc
from threading import Lock


class MSSQLConnector:
    _instance = None
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(MSSQLConnector, cls).__new__(cls)
        return cls._instance

    def __init__(self, server, database, username=None, password=None, trusted_connection=True):
        if hasattr(self, 'initialized'):
            return

        driver = 'ODBC Driver 17 for SQL Server'
        if trusted_connection:
            self.conn_str = (
                f'DRIVER={{{driver}}};'
                f'SERVER={server};'
                f'DATABASE={database};'
                f'Trusted_Connection=yes;'
            )
        else:
            self.conn_str = (
                f'DRIVER={{{driver}}};'
                f'SERVER={server};'
                f'DATABASE={database};'
                f'UID={username};'
                f'PWD={password};'
            )

        self.conn = pyodbc.connect(self.conn_str)
        self.initialized = True

    def get_cursor(self):
        if not self.conn:
            self.conn = pyodbc.connect(self.conn_str)
        return self.conn.cursor()

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None


connector = MSSQLConnector(server='localhost', database='brazilian_ecommerce')


if __name__ == "__main__":
    cursor = connector.get_cursor()

    cursor.execute("SELECT TOP 5 name FROM sys.databases")
    for row in cursor.fetchall():
        print(row)

    cursor.execute("SELECT TOP 5 name FROM sys.tables")
    for row in cursor.fetchall():
        print(row)
