import datetime
import oracledb
import pandas as pd
from auxiliares import Logger
import os
import time


class OracleCon:
    def __init__(self, lib_dir, db_user, password, tsn):
        #self.log = Logger('OracleCon')
        #self.log.information('Initializing OracleDB')

        self._user = db_user
        self._pwd = password
        self.TSN = tsn

        try:
            os.environ["PATH"] = f"{lib_dir};" + os.environ["PATH"]

            oracledb.init_oracle_client(lib_dir=lib_dir)
            #self.log.information('OracleDB initialized successfully')
        except Exception:
            #self.log.error('Error in oracledb client initialization')
            pass

        try:
            self._connection_test()
        except Exception:
            pass
            #self.log.logger.critical('Error to execute test connection')

        self.connection = None
        self.cursor = None

    def _connection_test(self):
        with oracledb.connect(user=self._user, password=self._pwd, dsn=self.TSN) as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT SYSDATE FROM DUAL')

                result = cursor.fetchone()

        if type(result[0]) is datetime.datetime:
            pass
            #self.log.information('Test connection was successful')
        else:
            pass
            #self.log.warning('Test connection do not return what should be')

    def create_connection(self):
        #self.log.information('Creating connection')

        while True:  # Loop created by gemini
            try:
                self.connection = oracledb.connect(user=self._user, password=self._pwd, dsn=self.TSN)
                #self.log.information('Conexão estabelecida com sucesso!')
                break  # Sai do loop se a conexão for bem-sucedida

            except oracledb.DatabaseError as db_error:
                if "ORA-02391" in str(db_error):  # Verifica se o erro é limite de conexões
                    #self.log.warning(
                     #   f'Limite de conexões atingido. Tentando novamente em 120 segundos... Detalhes do erro: {db_error}')
                    time.sleep(120)  # Aguarda 120 segundos antes de tentar novamente
                else:
                    #self.log.information(f'Erro ao conectar ao banco de dados! ERROR: {db_error}')
                    raise  # Lança a exceção para interromper a execução caso seja outro tipo de erro

    def close_connection(self):
        if self.is_connection_active():
            self.connection.close()

    def is_connection_active(self):
        try:
            aux = self.connection.username
            return True
        except oracledb.InterfaceError:
            return False
        except AttributeError:
            if self.connection is None:
                return False
        except Exception as UnhandledError:
            #self.log.warning(f'Unhandled error in connection check: {UnhandledError}')
            pass

    def create_cursor(self):
        self.cursor = self.connection.cursor()

    def close_cursor(self):
        self.cursor.close()

    def execute_select(self, sql: str, **kwargs) -> pd.DataFrame:
        var1 = kwargs.get('var1')
        var2 = kwargs.get('var2')

        columns = []

        try:
            if var1 and var2:
                self.cursor.execute(sql, myvar1=var1, myvar2=var2)
            if var1:
                if type(var1) == list:
                    self.cursor.execute(sql, var1)
                else:
                    self.cursor.execute(sql, myvar1=var1)
            else:
                self.cursor.execute(sql)
        except Exception as UnhandledError:
            #self.log.warning(f'Unhandled error while executing select: {UnhandledError}')

            # TODO: Create exception class
            # Raising exception to treat and log in main
            raise Exception

        # Get column names
        for column in self.cursor.description:
            columns.append(column[0])  # Append column name in columns index

        try:
            df = pd.DataFrame(self.cursor.fetchall(), columns=columns)
        except Exception as UnhandledError:
            #self.log.warning(f'Unhandled error while executing select: {UnhandledError}')
            raise pd.errors.DataError  # DataError is a place holder

        return df


if __name__ == '__main__':
    pass
