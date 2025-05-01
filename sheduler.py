from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend import JobHE, JobDE, Parameter

import schedule
from concurrent.futures import ThreadPoolExecutor

from oracle import OracleCon

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
main_parameters = open_json()

# Configuração do SQLITE3
SQLITE_PATH = os.getenv('SQLITE_PATH', main_parameters['backend']['sqlite_path'])
engine = create_engine(SQLITE_PATH, echo=False)
Session = sessionmaker(bind=engine)

# Thread pool (ajuste max_workers conforme CPUs / volume de jobs)
# TODO: Implementar numero de workers no datafile.json
executor = ThreadPoolExecutor(max_workers=5)

# Configuração Oracle 11g
# Create OracleDB object
"""
try:
    lib = main_parameters['database']['INSTANT_CLIENT']
    user = main_parameters['user_name']
    pwd = main_parameters['user_pass']
    dsn = main_parameters['database']['TSN']
    oracle_cnx = OracleCon(lib, user, pwd, dsn)
except Exception as UnhandledError:
    print(f'Erro ao inicializar conexão com Oracle: {UnhandledError}')
    exit(-1)
"""

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
                #'sql_script': job.sql_script,
                'day': s.job_day,
                'time': f"{s.job_hour.zfill(2)}:{s.job_minute.zfill(2)}"
            })
    session.close()
    return result

def execute_job(job_data):
    pass

def schedule_job(jobs=None):
    """
    - Se job for None: carrega TODOS os registros do banco e agenda cada um.
    - Se job for um dict: agenda apenas esse job em memória.
    - Se job for um int (job_id): busca esse registro no banco e agenda.
    """
    if jobs is None:
        jobs = fetch_jobs()
    elif type(jobs) == int:
        jobs = fetch_jobs(job_id=jobs)
    elif type(jobs) == dict:
        pass  # não faz nada, pois já está pronto a dict
    else:
        raise TypeError

    for job in jobs:
        day_key = job['day'].lower()
        tag = job['job_id']
        hhmm = job['time']

        day_method = DAY_MAP.get(day_key)
        if not day_method:
            print(f"Dia inválido para agendar: {job['job_day']}", "Scheduler")
            continue # pula par ao próximo item do loop

        def job_wrapper(job_data=job):
            executor.submit(execute_job, job_data)

        # schedule.every().monday.at("14:30").do(task).tag(tag)
        getattr(schedule.every(), day_method).at(hhmm).do(job_wrapper).tag(tag)

if __name__ == '__main__':
    pass
