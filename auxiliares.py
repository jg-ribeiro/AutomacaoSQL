import datetime
import locale
import os
import sys
import json
import logging
import pandas as pd
import win32com.client
from access import *
import re

locale.setlocale(locale.LC_TIME, 'pt_br')

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
Color print function
##----------------------------------------
"""


def color_print(string, color: str):
    colors = {
        'RED': '\033[91m',
        'GRE': '\033[92m',
        'BLU': '\033[96m',
        'YEL': '\033[93m'
    }
    color = colors[color]
    white = '\033[00m'

    print(color + string + white)


"""
##----------------------------------------
Json files aux function
##----------------------------------------
"""


def open_json() -> dict:
    base_content = """
{
  "access_path": "",
  "oracle_database": {
    "TSN": "",
    "INSTANT_CLIENT": ""
  },
  "backend": {
    "secret_key": "",
    "sqlite_path": ""
  },
  "archive_paths": "",
  "user_name": "",
  "user_pass": ""
}"""

    try:
        with open('datafile.json', 'r', encoding='utf-8') as jsonfile:
            datafile = jsonfile.read()
    except FileNotFoundError:
        color_print('Arquivo não encontrado!', 'RED')
        with open('datafile.json', 'w', encoding='utf-8') as jsonfile:
            jsonfile.writelines(base_content)
        exit()

    return json.loads(datafile)


"""
##----------------------------------------
Logging functions
##----------------------------------------
"""


class CustomFormatter(logging.Formatter):
    format = r"%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: format,
        logging.INFO: format,
        logging.WARNING: format,
        logging.ERROR: format,
        logging.CRITICAL: format,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class Logger:
    def __init__(self, name: str, db_manager=None):
        """
        :param name: log identification
        """

        if db_manager is None:
            print('WARNING:' + name + ':Access database was not configured')

        self._db_manager = db_manager

        logging.basicConfig(
            filename='informacoes.log',
            encoding='utf-8',
            level=logging.INFO
        )
        self.logger = logging.getLogger(name)
        self._log_name = name

        # Adiciona um manipulador de console com o formatador personalizado:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(CustomFormatter())
        self.logger.addHandler(console_handler)

    @property
    def db_manager(self):
        return self._db_manager

    @db_manager.setter
    def db_manager(self, db_manager: DBManager):
        self._db_manager = db_manager
        print('INFO:' + self._log_name + ':Access database has successfully set')

    def information(self, msg: str, job_info: str):
        self.logger.info(msg)
        self._log_to_database('INFO', job_info, msg)

    def warning(self, msg: str, job_info: str):
        self.logger.warning(msg)
        self._log_to_database('WARNING', job_info, msg)

    def error(self, msg: str, job_info: str):
        self.logger.error(msg)
        self._log_to_database('ERROR', job_info, msg)

    def internal_logger_error(self, msg: str):
        self.logger.error(msg)

    def _log_to_database(self, level: str, job_info: str, msg: str):
        """
        Add record to log table
        :param level: Log level info
        :param job_info: Job info. Obs.: Can be job name, type of work being done
        :param msg: Error message to be stored in access table
        :return:
        """
        if self.db_manager:
            try:
                self.db_manager.execute_dml(
                    "INSERT INTO TABELA_LOG ([TIMESTAMP], LOG_LEVEL, LOG_NAME, WORK, LOG_TEXT)\n"
                    f"VALUES ('{str(get_datetime())}', '{level}', '{self._log_name}', '{job_info}', '{msg}')"
                )
            except Exception as ex:
                self.internal_logger_error(f"Erro ao registrar no banco de dados: {ex}")

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
