from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend import JobHE, JobDE, Parameter

import oracledb

import schedule
from concurrent.futures import ThreadPoolExecutor

import csv
import re

import datetime
import time

from auxiliares import *


## CONSTANTES

# abreviações PT → método schedule em inglês
DAY_MAP = {
    'Seg': 'monday',
    'Ter': 'tuesday',
    'Qua': 'wednesday',
    'Qui': 'thursday',
    'Sex': 'friday',
    'Sab': 'saturday',
    'Dom': 'sunday'
}

# Parametros principais
MAIN_PARAMETERS = open_json()

# Parametros do oracle
LIB = MAIN_PARAMETERS['oracle_database']['INSTANT_CLIENT']
DSN = MAIN_PARAMETERS['oracle_database']['TSN']
USER = MAIN_PARAMETERS['user_name']
PWD = MAIN_PARAMETERS['user_pass']
ARRAYSIZE = 5000

# Configuração do SQLITE3
SQLITE_PATH = os.getenv('SQLITE_PATH', MAIN_PARAMETERS['backend']['sqlite_path'])
engine = create_engine(SQLITE_PATH, echo=False)
Session = sessionmaker(bind=engine)

# Thread pool (ajuste max_workers conforme CPUs / volume de jobs)
# TODO: Implementar numero de workers no datafile.json
executor = ThreadPoolExecutor(max_workers=5)

# Configuração Oracle 11g
# Create OracleDB object

try:
    oracledb.init_oracle_client(lib_dir=LIB)
    # pequena conexão teste:
    with oracledb.connect(user=USER, password=PWD, dsn=DSN) as connection:
        with connection.cursor() as cursor:
            cursor.execute('SELECT SYSDATE FROM DUAL')

            result = cursor.fetchone()
    
    if not type(result[0]) is datetime.datetime:
        raise ConnectionError('Erro ao executar DQL teste')
except Exception:
    print(f'Erro ao inicializar conexão com Oracle: {Exception}')
    exit(-1)

def fetch_parameter(parameter_id: int):
    session = Session()

    param = session.query(Parameter).filter_by(parameter_id=parameter_id).first()

    result = {
            'parameter_id': param.parameter_id,
            'parameter_name': param.parameter_name,
            'sql_script': param.sql_script
        }

    return result


def fetch_jobs(job_id=None):
    session = Session()
    # só traz aqueles com status = 'Y' (ativo)

    if job_id is None:
        jobs = session.query(JobHE).filter_by(job_status='Y').all()
    else:
        jobs = session.query(JobHE).filter_by(job_status='Y', job_id=job_id).all()

    result = []
    for job in jobs:
        # coleta os horários/dias associados
        scheds = session.query(JobDE).filter_by(job_id=job.job_id).all()
        for s in scheds:
            result.append({
                'job_id': job.job_id,
                'schedule_id': s.schedule_id,
                'name': job.job_name,
                'export_type': job.export_type,
                'export_name': job.export_name,
                'export_path': job.export_path,
                'days_offset': job.days_offset,
                'check_parameter': job.check_parameter,
                'parameter_id': job.parameter_id,
                'data_primary_key': job.data_primary_key,
                'sql_script': job.sql_script,
                'day': s.job_day,
                'time': f"{s.job_hour.zfill(2)}:{s.job_minute.zfill(2)}"
            })
    session.close()
    return result

def execute_job(job_data):
    print('iniciando trabalho:', job_data['name'])
    try:
        name = job_data['name']
        hour = job_data['time']
        accum_type = job_data['export_type']
        days_offset = int(job_data['days_offset'])
        archive_path = job_data['export_path']
        archive_name = job_data['export_name']
        archive_name_with_extention = job_data['export_name'] + '.csv'
        check_parameter = job_data['check_parameter']
        if check_parameter:
            parameter_id = job_data['parameter_id']
        else:
            parameter_id = None

        data_primary_key = job_data['data_primary_key']
        sql = job_data['sql_script']
    except Exception as error:
        print('Erro na leitura dos dados', error)
        return
    
    absolute_path = os.path.join(archive_path, archive_name_with_extention)
    
    # Verifica se o comando é DQL
    if not is_select_query(sql):
        raise ValueError('Apenas consultas DQL (SELECT) são permitidas.')
        # Ou fazer log no banco

    # Execução do SQL e exportação com fetchmany()
    with oracledb.connect(user=USER, password=PWD, dsn=DSN) as connection:
        with connection.cursor() as cursor:
            cursor.arraysize = ARRAYSIZE
            cursor.execute(sql)

            with open(absolute_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                headers = [col[0] for col in cursor.description]
                writer.writerow(headers)
                
                # busca em blocos de até `arraysize`
                while True:
                    rows = cursor.fetchmany()  # vai até `arraysize` linhas
                    if not rows:
                        break
                    writer.writerows(rows)


def schedule_job(jobs=None):
    """
    - Se job for None: carrega TODOS os registros do banco e agenda cada um.
    - Se job for um dict: agenda apenas esse job em memória.
    - Se job for um int (job_id): busca esse registro no banco e agenda.
    """
    if jobs is None:
        jobs = fetch_jobs()
        print('Iniciando Scheduler Total')
    elif type(jobs) == int:
        jobs = fetch_jobs(job_id=jobs)
        print(f'Iniciando Schedule por ID: {jobs}')
    elif type(jobs) == dict:
        print(f'Iniciando Schedule por dict: {jobs['name']}')
        # não faz nada, pois já está pronto a dict
    else:
        raise TypeError

    for job in jobs:
        day_key = job['day']
        tag = job['job_id']
        hhmm = job['time']

        day_method = DAY_MAP.get(day_key)
        if not day_method:
            print(f"Dia inválido para agendar: {job['day']}", "Scheduler")
            continue # pula par ao próximo item do loop

        def job_wrapper(job_data=job):
            executor.submit(execute_job, job_data)

        print(f'Agendando', job['name'], day_key, 'as', hhmm)
        # schedule.every().monday.at("14:30").do(task).tag(tag)
        getattr(schedule.every(), day_method).at(hhmm).do(job_wrapper).tag(tag)


def run_loop():
    print('iniciando run_loop')
    print(schedule.next_run())
    while True:
        # Executa o que está pendente
        schedule.run_pending()

        idle = schedule.idle_seconds()
        if idle is None:
            # Não há nenhum trabalho, então dorme
            print('longsleep', datetime.datetime.now())
            time.sleep(120)
        else:
            # Dorme só o necessário, minimo de 1 segundo
            print('shortsleep', datetime.datetime.now())
            time.sleep(10)


# a cada 2 horas, faz o reload completo:
schedule.every(2).hours.do(lambda: (
    schedule.clear(),   # limpa tudo
    schedule_job()      # recarrega todos
))


if __name__ == '__main__':
    schedule_job()
    run_loop()