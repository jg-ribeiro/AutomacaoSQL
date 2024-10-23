import pyodbc
import pandas as pd
import warnings


class DBManager:
    def __init__(self, db_path: str):
        # Ignore pandas ODBC warning
        warnings.filterwarnings('ignore')
        self._conn_str = r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + db_path + ';'

        try:
            self.conn = pyodbc.connect(self._conn_str)
        except Exception as e:
            self._conn_str = r'Driver={Driver do Microsoft Access (*.mdb)};DBQ=' + db_path + ';'

            self.conn = pyodbc.connect(self._conn_str)

        self.conn.close()

    def _open_connection(self):
        self.conn = pyodbc.connect(self._conn_str)

    def execute_dql(self, sql: str) -> pd.DataFrame:
        self._open_connection()

        df = pd.read_sql(sql, self.conn)

        # Close connection
        self.conn.close()
        return df

    def execute_dml(self, sql: str):
        self._open_connection()

        cursor = self.conn.cursor()
        cursor.execute(sql)

        self.conn.commit()

        # Close connection
        cursor.close()
        self.conn.close()


if __name__ == '__main__':
    pass
