from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from backend import JobHE, JobDE, Parameter

import oracledb

import schedule
from concurrent.futures import ThreadPoolExecutor

import csv

import datetime
import time

from auxiliares import *

# --- Import Logging ---
from logging_config import get_logger, log_info, log_warning, log_error, log_exception, log_debug, MAIN_PARAMETERS
logger = get_logger('scheduler')
# --- End Logging Import ---

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

    log_debug(logger, f"Fetching jobs from DB. Specific job_id: {job_id if job_id else 'All active'}")

    try:
        # só traz aqueles com status = 'Y' (ativo)
        if job_id is None:
            jobs = session.query(JobHE).filter_by(job_status='Y').all()
        else:
            jobs = session.query(JobHE).filter_by(job_status='Y', job_id=job_id).all()

        result = []

        job_ids_fetched = [j.job_id for j in jobs]
        if not job_ids_fetched:
                log_debug(logger, "No active jobs found matching criteria.")
                return result # Vazio

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
        
        log_debug(logger, f"Fetched {len(result)} job schedule instances.")
        return result
    except Exception as e:
        log_exception(logger, f"Error fetching jobs from database: {e}")
        return [] # Return empty list on error
    finally:
        session.close()


def execute_job(job_data):
    job_id = job_data.get('job_id', None)
    job_name = job_data.get('name', 'Unknown Job')
    start_time = time.time()
    log_info(logger, f"Starting job execution: '{job_name}'", job_id=job_id)

    try:
        accum_type = job_data['export_type']
        days_offset = int(job_data['days_offset'])
        archive_path = job_data['export_path']
        archive_name_with_extention = job_data['export_name'] + '.csv'
        check_parameter = job_data['check_parameter']
        if check_parameter:
            parameter_id = job_data['parameter_id']
        else:
            parameter_id = None

        data_primary_key = job_data['data_primary_key']
        sql = job_data['sql_script']

        if not sql:
            log_error(logger, f"Job '{job_name}' has no SQL script defined.", job_id=job_id)
            return
    
        absolute_path = os.path.join(archive_path, archive_name_with_extention)
        log_debug(logger, f"Job '{job_name}': Export path: {absolute_path}", job_id=job_id)

        # Ensure target directory exists
        os.makedirs(archive_path, exist_ok=True)

        # Verifica se o comando é DQL
        if not is_select_query(sql):
            log_error(logger, f"Job '{job_name}': SQL is not a SELECT query. Aborting.", job_id=job_id)
            return

        log_debug(logger, f"Job '{job_name}': Executing SQL:\n{sql[:200]}...", job_id=job_id)
        rows_exported = 0

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
                        rows_exported += len(rows)
                        log_debug(logger, f"Job '{job_name}': Fetched/wrote {len(rows)} rows (Total: {rows_exported})", job_id=job_id)
        
        end_time = time.time()
        duration_ms = int((end_time - start_time) * 1000)
        log_info(logger, f"Job '{job_name}' finished successfully. Exported {rows_exported} rows.", job_id=job_id, duration_ms=duration_ms)

    except FileNotFoundError:
        log_exception(logger, f"Job '{job_name}': Error creating/writing file at '{absolute_path}'. Check path and permissions.", job_id=job_id)
    except oracledb.DatabaseError as ora_err:
         log_exception(logger, f"Job '{job_name}': Oracle Database Error during execution: {ora_err}", job_id=job_id)
    except Exception as error:
        end_time = time.time()
        duration_ms = int((end_time - start_time) * 1000)
        # Use log_exception to include traceback
        log_exception(logger, f"Job '{job_name}': Unexpected error during execution: {error}", job_id=job_id, duration_ms=duration_ms)
        # Optionally re-raise if needed elsewhere, but likely not in a scheduled task
        # return # Ensure function exits on error

def schedule_job(jobs=None):
    """
    - Se job for None: carrega TODOS os registros do banco e agenda cada um.
    - Se job for um dict: agenda apenas esse job em memória.
    - Se job for um int (job_id): busca esse registro no banco e agenda.
    """
    log_source = "database (all active)"
    if jobs is None:
        jobs = fetch_jobs()
        log_info(logger, f"Scheduling all active jobs from database.")
    elif type(jobs) == int:
        log_source = f"database (ID: {jobs})"
        jobs = fetch_jobs(job_id=jobs)
        log_info(logger, f"Scheduling specific job from database: ID {jobs}.")
    elif type(jobs) == dict:
        log_source = "provided list"
        log_info(logger, f"Scheduling {len(jobs)} jobs from provided list.")
        # Assume list of dicts is correctly formatted
    else:
        log_error(logger, f"Invalid input type for schedule_job: {type(jobs)}. Expected None, int, or list[dict].")
        return
    
    scheduled_count = 0

    try:
        for job in jobs:
            day_key = job['day']
            tag = job['job_id']
            hhmm = job['time']

            day_method = DAY_MAP.get(day_key)
            if not day_method:
                log_warning(logger, f"Invalid day '{day_key}' for job '{job['name']}' (ID: {job['job_id']}). Skipping this schedule.", job_id=job['job_id'])
                continue

            def job_wrapper(job_data=job):
                log_debug(logger, f"Submitting job '{job_data['name']}' (ID: {job_data['job_id']}) to executor.", job_id=job_data['job_id'])
                executor.submit(execute_job, job_data)

            log_info(logger, f"Scheduling job '{job['name']}' (Tag: {tag}) for {day_key} at {hhmm}", job_id=job['job_id'])
            # schedule.every().monday.at("14:30").do(task).tag(tag)
            getattr(schedule.every(), day_method).at(hhmm).do(job_wrapper).tag(tag)
            scheduled_count += 1
    except KeyError as e:
            log_error(logger, f"Missing key {e} in job data while scheduling: {job}. Skipping this schedule.", job_id=job.get('job_id'))
    except Exception as e:
            log_exception(logger, f"Unexpected error scheduling job {job.get('name', 'Unknown')}: {e}", job_id=job.get('job_id'))

    log_info(logger, f"Finished scheduling. Added {scheduled_count} schedule entries.")


def run_loop():
    log_info(logger, "Scheduler run_loop starting.")
    log_info(logger, f"Next scheduled run at: {schedule.next_run}")
    while True:
        try:
            schedule.run_pending()

            idle = schedule.idle_seconds()
            if idle is None:
                # No jobs scheduled
                log_debug(logger, "No jobs scheduled. Sleeping for 120 seconds.")
                time.sleep(120)
            elif idle > 0:
                # Sleep until the next job, but check more frequently than idle_seconds
                # Check every 60 seconds or until next job, whichever is smaller
                sleep_time = min(idle, 60)
                log_debug(logger, f"Next job in {idle:.2f} seconds. Sleeping for {sleep_time:.2f} seconds.")
                time.sleep(sleep_time)
            else:
                # Jobs might be due now or overdue, sleep very briefly
                 log_debug(logger, "Jobs pending or due. Short sleep (1s).")
                 time.sleep(10) # Short sleep if jobs ran or are due

        except KeyboardInterrupt:
             log_info(logger, "Scheduler run_loop interrupted by user (KeyboardInterrupt). Exiting.")
             break
        except Exception as e:
             log_exception(logger, f"Error in scheduler run_loop: {e}. Continuing loop.")
             time.sleep(30) # Sleep a bit longer after an error in the loop itself

# a cada 2 horas, faz o reload completo:
schedule.every(2).hours.do(lambda: (
    schedule.clear(),   # limpa tudo
    schedule_job()      # recarrega todos
))
log_info(logger, "Scheduled periodic job reload every 2 hours.")


if __name__ == '__main__':
    log_info(logger, "*** Scheduler Service Starting ***")
    try:
        schedule_job() # Initial scheduling
        run_loop()
    except Exception as e:
        log_exception(logger, "*** Scheduler Service Crashed Unhandled Exception ***")
    finally:
        log_info(logger, "*** Scheduler Service Shutting Down ***")
        executor.shutdown(wait=True) # Wait for running jobs to finish if possible