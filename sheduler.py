from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend import JobHE, JobDE, Parameter 

from oracle import OracleCon

from auxiliares import *


# Parametros principais
main_parameters = open_json()

# Configuração do SQLITE3
SQLITE_PATH = os.getenv('SQLITE_PATH', main_parameters['backend']['sqlite_path'])
engine = create_engine(SQLITE_PATH, echo=False)
Session = sessionmaker(bind=engine)

# Configuração Oracle 11g
# Create OracleDB object
try:
    lib = main_parameters['database']['INSTANT_CLIENT']
    user = main_parameters['user_name']
    pwd = main_parameters['user_pass']
    dsn = main_parameters['database']['TSN']
    oracle_cnx = OracleCon(lib, user, pwd, dsn)
except Exception as UnhandledError:
    print(f'Erro ao inicializar conexão com Oracle: {UnhandledError}')
    exit(-1)



if __name__ == '__main__':
    pass