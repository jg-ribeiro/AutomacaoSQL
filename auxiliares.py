import datetime
import locale
import os
import sys
import json
import re
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

locale.setlocale(locale.LC_TIME, 'pt_br')

def get_postgres_engine(pg_params):
    """Cria e retorna um engine SQLAlchemy para o PostgreSQL."""
    password_safe = quote_plus(pg_params['password'])
    database_url = (
        f"postgresql+psycopg2://{pg_params['username']}:{password_safe}"
        f"@{pg_params['hostname']}:{pg_params['port']}/{pg_params['database']}"
    )
    engine = create_engine(database_url)
    
    # Testa a conexão
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version()"))
            print(f"Conectado com sucesso ao PostgreSQL: {result.fetchone()[0]}")
        return engine
    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        exit(1)

def get_postgres_url(pg_params):
    password_safe = quote_plus(pg_params['password'])
    database_url = (
        f"postgresql+psycopg2://{pg_params['username']}:{password_safe}"
        f"@{pg_params['hostname']}:{pg_params['port']}/{pg_params['database']}"
    )

    return database_url

"""
##----------------------------------------
Path aux functions
##----------------------------------------
"""


def resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


"""
##----------------------------------------
Date and time aux functions
##----------------------------------------
"""


def get_datetime() -> datetime.datetime:
    return datetime.datetime.now()


def get_time(time_value: str) -> datetime.datetime:
    return datetime.datetime.strptime(time_value, '%H:%M')


def getdate_df_format(date_value: datetime) -> str:
    return date_value.strftime('%Y-%m-%d')


def getdate_str(date_value: datetime) -> str:
    return date_value.strftime('%d/%m/%Y')


def date_treatment(last_execution: str, days_requirement: int) -> dict:
    today_date = datetime.date.today()

    initial_date = (
            datetime.datetime.strptime(last_execution, '%d/%m/%Y') + datetime.timedelta(days=1)
    ).replace(day=1)

    final_date = today_date - datetime.timedelta(days=days_requirement)

    month_check = initial_date.strftime('%m') != final_date.strftime('%m')

    data = {
        'sql_dates': [initial_date, final_date],
        'today_date': today_date,
        'month_check': month_check
    }

    return data


def get_export_name(arch_name: str, date_ref: datetime.datetime) -> str:
    month_str = date_ref.strftime(' %m.%Y')
    export_name = arch_name + month_str + '.csv'
    return export_name


"""
##----------------------------------------
Json files aux function
##----------------------------------------
"""


def open_json() -> dict:
    base_content = """{
  "oracle_database": {
    "TSN": "",
    "INSTANT_CLIENT": ""
  },
  "backend": {
    "secret_key": "",
    "sqlite_path": ""
  },
  "postgres":{
    "hostname": "localhost",
    "port": "5432",
    "database": "",
    "username": "",
    "password": ""
  },
  "user_name": "",
  "user_pass": "",
  "data_api": {
    "csv_folder_path": "",
    "api_keys": [
      ""
    ]
  }
}"""

    try:
        with open('datafile.json', 'r', encoding='utf-8') as jsonfile:
            datafile = jsonfile.read()
    except FileNotFoundError:
        print('Arquivo não encontrado!')
        with open('datafile.json', 'w', encoding='utf-8') as jsonfile:
            jsonfile.writelines(base_content)
        exit()

    return json.loads(datafile)


"""
##----------------------------------------
SQL check functions
##----------------------------------------
"""

def is_select_query(sql):
    """
    Verifica se a consulta SQL é apenas para leitura (DQL).
    """
    # Remove comentários e normaliza espaços em branco
    normalized_sql = re.sub(r'--.*?\n|/\*.*?\*/', '', sql, flags=re.DOTALL)
    normalized_sql = re.sub(r'\s+', ' ', normalized_sql).strip().upper()
    
    # Verifica se a consulta começa com palavras-chave de leitura
    allowed_patterns = [
        r'^SELECT\s+',
        r'^WITH\s+',
        r'^SHOW\s+',
        r'^DESCRIBE\s+',
        r'^EXPLAIN\s+'
    ]
    
    # Verifica se a consulta é apenas de leitura
    is_read_only = any(re.match(pattern, normalized_sql) for pattern in allowed_patterns)
    
    # Verifica se não contém palavras-chave de modificação de dados
    forbidden_patterns = [
        r'\s+INSERT\s+',
        r'\s+UPDATE\s+', 
        r'\s+DELETE\s+',
        r'\s+DROP\s+',
        r'\s+CREATE\s+',
        r'\s+ALTER\s+',
        r'\s+TRUNCATE\s+',
        r'\s+GRANT\s+',
        r'\s+REVOKE\s+',
        r'\s+MERGE\s+',
        r'^INSERT\s+', 
        r'^UPDATE\s+', 
        r'^DELETE\s+',
        r'^DROP\s+',
        r'^CREATE\s+',
        r'^ALTER\s+',
        r'^TRUNCATE\s+',
        r'^GRANT\s+',
        r'^REVOKE\s+',
        r'^MERGE\s+'
    ]
    
    contains_forbidden = any(re.search(pattern, normalized_sql) for pattern in forbidden_patterns)
    
    return is_read_only and not contains_forbidden


if __name__ == '__main__':
    open_json()
