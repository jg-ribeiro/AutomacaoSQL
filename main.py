import datetime
import time
from access import DBManager
from oracle import OracleCon
from auxiliares import *
import schedule
import pandas as pd


def accum_processing(arch_name: str, att_df, initial_date: datetime.datetime, date_key: str):
    """
    function responsible for processing and exporting files of the accumulated type
    :param arch_name: csv file with extension ex.: 'AptCargas.csv'
    :param att_df: DataFrame get from select operation
    :param initial_date: First date of sql select instruction
    :param date_key: Name of the date column
    :return: void
    """
    global main_parameters, database

    # Parse char to datetime
    att_df[date_key] = pd.to_datetime(att_df[date_key], format='%d/%m/%Y')

    try:
        accum_df = pd.read_csv(os.path.join(main_parameters['archive_paths'], arch_name), encoding='utf-8', sep=';')

        # Parse char to datetime, because the way pandas export, this needs to be different
        accum_df[date_key] = pd.to_datetime(accum_df[date_key], format='%Y-%m-%d')
    except FileNotFoundError:
        # Export csv anyway
        att_df.to_csv(os.path.join(main_parameters['archive_paths'], arch_name), encoding='utf-8', sep=';', index=False)
    else:
        # Get index of items to be deleted
        drop_index = accum_df[accum_df[date_key] >= initial_date].index

        # Remove selected lines
        accum_df.drop(index=drop_index, inplace=True)

        # Export dataframe concatenated
        pd.concat([accum_df, att_df]).to_csv(
            os.path.join(main_parameters['archive_paths'], arch_name),
            encoding='utf-8', sep=';', index=False  # Export parameters
        )


def month_processing(arch_name: str, data_frame: pd.DataFrame, key: str):
    """
    function responsible for processing and exporting files of the month type
    :param arch_name: arch_name: csv file without extension ex.: 'AptCargas'
    :param data_frame: DataFrame get from select operation
    :param key: Name of the date column
    :return: void
    """
    global main_parameters, database

    # Parse column type to pd.Datetime
    data_frame[key] = pd.to_datetime(data_frame[key], format='%d/%m/%Y')

    # Get unique month number
    months = data_frame[key].dt.month.unique()

    for month in months:
        month_df = data_frame[data_frame[key].dt.month == month]

        first_date = month_df[key].min()
        month_str = get_export_name(arch_name, first_date)

        month_df.to_csv(
            os.path.join(main_parameters['archive_paths'], month_str),  # Archive path with name
            index=False, encoding='utf-8', sep=';'  # Export parameters
        )


def default_processing(arch_name: str, data_frame: pd.DataFrame, key=None):
    global main_parameters

    # Parse column type to pd.Datetime
    if key is not None:
        data_frame[key] = pd.to_datetime(data_frame[key], format='%d/%m/%Y')

    data_frame.to_csv(
        os.path.join(main_parameters['archive_paths'], arch_name),
        index=False, encoding='utf-8', sep=';'  # Export parameters
    )


def execute_job(raw_data, iter_number=0):
    global parameters_script, oracle_cnx, database, main_parameters

    start_job_time = get_datetime()

    try:
        name = raw_data['NOME']
        hour = raw_data['HORA']
        accum_type = raw_data['TIPO_ARQUIVO']
        days_grace = int(raw_data['DIAS_CARENCIA'])
        archive_name = raw_data['NOME_ARQUIVO']
        archive_name_with_extention = raw_data['NOME_ARQUIVO'] + '.csv'
        parameter_name = raw_data['NOME_PARAMETRO']
        date_key = raw_data['COLUNA_DATA']
        sql = raw_data['SCRIPT']
        mail_recipients = raw_data['RESPONSAVEIS'].split(',')
        last_date = raw_data['ULT_DATA']
    except Exception as error:
        logger.information(
            f'Erro de leitura das informações: {name if name else None}',
            'Eventual Initialization'
        )

        logger.error(str(error), 'Routine Initialization')
        return

    logger.information(
        f'Iniciando trabalho {name}',
        'Routine Info'
    )

    if not accum_type == 'Único':
        datas_sql = date_treatment(last_date, days_grace)
        date_comparison = getdate_df_format(datas_sql['sql_dates'][1])
    else:
        datas_sql = None
        date_comparison = None

    try:
        if parameter_name is not None:
            parameter_df = oracle_cnx.execute_select(parameters_script[parameter_name]['SCRIPT'])
            parameter_df['VALOR'] = pd.to_datetime(parameter_df['VALOR'], format='%d/%m/%Y')  # Convert columns to date

            filtered_df = parameter_df[parameter_df['VALOR'] < date_comparison]
            filtered_df_len = len(filtered_df)
            filtered_df_unidades = ', '.join(str(valor) for valor in filtered_df['INSTANCIA'])

            if filtered_df_len >= 1:
                if iter_number < 1:
                    hour = (get_time(hour) + datetime.timedelta(minutes=10)).strftime('%H:%M')
                    raw_data['HORA'] = hour

                    schedule.every().day.at(hour).do(execute_job, raw_data=raw_data, iter_number=iter_number + 1)

                    logger.information(
                        f'Trabalho re-agendado parametro {parameter_name} aberto em: {filtered_df_unidades}',
                        'Routine Re-Schedule'
                    )

                    if iter_number > 0:
                        return schedule.CancelJob  # Return CancelJob to run this job only once
                    else:
                        return
                else:
                    logger.information(
                        f'Trabalho cancelado: {parameter_name}',
                        'Routine Cancel'
                    )

                    return schedule.CancelJob  # Return CancelJob to run this job only once
    except Exception as error:
        logger.information(
            f'Erro ao processar parametro :{parameter_name}',
            'Parameter Error'
        )
        return

    # se o df de filtro for zero, então procede para execução do select
    try:
        if datas_sql is not None:
            select_df = oracle_cnx.execute_select(sql, var1=datas_sql['sql_dates'])
        else:
            select_df = oracle_cnx.execute_select(sql)
    except Exception as error:
        logger.information(
            f'Erro no SQL: {parameter_name}',
            'SQL Error'
        )

        logger.error(
            str(error),
            'SQL Error'
        )
        return

    total_time = get_datetime() - start_job_time
    logger.information(
        f'Tempo de execução do SQL: {total_time}',
        'Routine Info'
    )
    sub_time = get_datetime()

    try:
        # Export csv
        if accum_type == 'Acumulado':
            accum_processing(archive_name_with_extention, select_df, datas_sql['sql_dates'][0], date_key)
        elif accum_type == 'Mês':
            month_processing(archive_name, select_df, date_key)
        elif accum_type == 'Único':
            default_processing(archive_name_with_extention, select_df)
        else:
            raise Exception

        total_time = get_datetime() - sub_time
        sub_time = get_datetime()

        logger.information(
            f'Tempo de processamento: {total_time}',
            'Routine Info'
        )
    except Exception as error:
        total_time = get_datetime() - start_job_time

        logger.information(
            f'Erro no processamento: {name}, tempo total: {total_time}',
            'Routine Info'
        )

        logger.error(
            str(error),
            'Routine Info'
        )
        return

    try:
        # Atualiza a database
        if datas_sql is not None:
            sql = (
                f"UPDATE ROTINAS \n"
                f"SET DATA_ULT_EXEC='{datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}',\n"
                f"ULT_DATA='{datas_sql['sql_dates'][1].strftime('%d/%m/%Y')}'\n"
                f"WHERE NOME='{name}'"
            )

            database.execute_dml(sql)
        else:
            sql = (
                f"UPDATE ROTINAS\n"
                f"SET DATA_ULT_EXEC='{datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}'\n"
                f"WHERE NOME='{name}'"
            )

            database.execute_dml(sql)
    except Exception as error:
        logger.error(f'Erro ao atualizar a base de: {raw_data[0]}', 'Routine Info')

    total_time = get_datetime() - start_job_time

    logger.information(
        f'Trabalho finalizado: {name}, tempo total: {total_time}',
        'Routine Info'
    )


def eventual_job(raw_data):
    global oracle_cnx, database, main_parameters, logger

    start_job_time = get_datetime()

    name = raw_data['NOME_ARQUIVO']
    archive_name = str(raw_data['NOME_ARQUIVO'] + '.csv')
    script = raw_data['SCRIPT']

    try:
        select_df = oracle_cnx.execute_select(script)

        select_df.to_csv(
            os.path.join(main_parameters['archive_paths'], archive_name),
            sep=';',
            encoding='utf-8',
            index=False
        )
    except Exception as Error:
        logger.error(f'Erro ao executar SQL eventual {name}: {Error}', 'Eventual')
    finally:
        # Delete job line
        sql = f"DELETE * FROM EVENTUAIS WHERE NOME_ARQUIVO='{name}'"

        database.execute_dml(sql)

        total_time = get_datetime() - start_job_time
        logger.information(f'Eventual finalizado {name}, tempo de execução: {total_time}', 'Eventual')


def has_day(row, day):
    return day in row['DIAS_EXEC']


def schedule_jobs():
    global executions, parameters_script

    logger.information('Starting scheduling!', 'Scheduling Info')

    schedule.clear()
    update_executions()

    current_day = 'Todos'
    current_day_executions = executions[executions.apply(lambda row: has_day(row, current_day), axis=1)]

    if len(current_day_executions) > 0:
        for index, script in current_day_executions.iterrows():
            script_hour = script['HORA']

            # comando que agenda a tarefa
            schedule.every().day.at(script_hour).do(execute_job, raw_data=script)

    current_day = 'Domingo'
    current_day_executions = executions[executions.apply(lambda row: has_day(row, current_day), axis=1)]

    if len(current_day_executions) > 0:
        for index, script in current_day_executions.iterrows():
            script_hour = script['HORA']

            # comando que agenda a tarefa
            schedule.every().sunday.at(script_hour).do(execute_job, raw_data=script)

    current_day = 'Segunda'
    current_day_executions = executions[executions.apply(lambda row: has_day(row, current_day), axis=1)]

    if len(current_day_executions) > 0:
        for index, script in current_day_executions.iterrows():
            script_hour = script['HORA']

            # comando que agenda a tarefa
            schedule.every().monday.at(script_hour).do(execute_job, raw_data=script)

    current_day = 'Terça'
    current_day_executions = executions[executions.apply(lambda row: has_day(row, current_day), axis=1)]

    if len(current_day_executions) > 0:
        for index, script in current_day_executions.iterrows():
            script_hour = script['HORA']

            # comando que agenda a tarefa
            schedule.every().tuesday.at(script_hour).do(execute_job, raw_data=script)

    current_day = 'Quarta'
    current_day_executions = executions[executions.apply(lambda row: has_day(row, current_day), axis=1)]

    if len(current_day_executions) > 0:
        for index, script in current_day_executions.iterrows():
            script_hour = script['HORA']

            # comando que agenda a tarefa
            schedule.every().wednesday.at(script_hour).do(execute_job, raw_data=script)

    current_day = 'Quinta'
    current_day_executions = executions[executions.apply(lambda row: has_day(row, current_day), axis=1)]

    if len(current_day_executions) > 0:
        for index, script in current_day_executions.iterrows():
            script_hour = script['HORA']

            # comando que agenda a tarefa
            schedule.every().thursday.at(script_hour).do(execute_job, raw_data=script)

    current_day = 'Sexta'
    current_day_executions = executions[executions.apply(lambda row: has_day(row, current_day), axis=1)]

    if len(current_day_executions) > 0:
        for index, script in current_day_executions.iterrows():
            script_hour = script['HORA']

            # comando que agenda a tarefa
            schedule.every().friday.at(script_hour).do(execute_job, raw_data=script)

    current_day = 'Sabado'
    current_day_executions = executions[executions.apply(lambda row: has_day(row, current_day), axis=1)]

    if len(current_day_executions) > 0:
        for index, script in current_day_executions.iterrows():
            script_hour = script['HORA']

            # comando que agenda a tarefa
            schedule.every().saturday.at(script_hour).do(execute_job, raw_data=script)


def has_pending_jobs(scheduled_jobs) -> bool:
    today = datetime.datetime.today()
    today_jobs = []
    pend_jobs = []

    for job in scheduled_jobs:
        if job.next_run.date() == today.date():
            today_jobs.append(job.next_run)

    for job in today_jobs:
        if job < today:
            pend_jobs.append(job)

    return bool(pend_jobs)


def update_executions():
    global executions, parameters_script

    # Update executions dataframe
    sql = r"SELECT * FROM ROTINAS"
    executions = database.execute_dql(sql)

    # Update parameters dict
    sql = r"SELECT * FROM PARAMETROS"
    parameters_script = database.execute_dql(sql).set_index('NOME').T.to_dict()


def update_eventual():
    global eventual

    sql = "SELECT * FROM EVENTUAIS"
    eventual = database.execute_dql(sql)

    return eventual.empty


if __name__ == '__main__':
    # Define o log principal
    logger = Logger(name='main')

    # Parametros principais
    main_parameters = open_json()

    if not os.path.exists(main_parameters['archive_paths']):
        os.mkdir(main_parameters['archive_paths'])
        logger.information(f'Created main dir: {main_parameters["archive_paths"]}', 'Initialization')

    logger.information('Initialing main schedule', 'Schedule')

    # Create Access object
    try:
        database = DBManager(main_parameters['access_path'])
    except Exception as UnhandledError:
        logger.information('Error to initialize Access Database', 'Database Initialization')
        logger.error(str(UnhandledError), 'Database Initialization')
        exit(-1)

    logger.db_manager = database

    # aux variables
    executions = pd.DataFrame()
    parameters_script = pd.DataFrame()
    eventual = pd.DataFrame()

    # Create OracleDB object
    try:
        lib = main_parameters['database']['INSTANT_CLIENT']
        user = main_parameters['user_name']
        pwd = main_parameters['user_pass']
        dsn = main_parameters['database']['TSN']
        oracle_cnx = OracleCon(lib, user, pwd, dsn)
    except Exception as UnhandledError:
        logger.error(f'Cant connect to oracle database: {UnhandledError}', 'Oracle Initialization')
        exit(-1)

    # Initial scheduling of jobs get from sharepoint
    schedule_jobs()

    # Schedule sharepoint data actualization
    schedule.every().day.at("00:00").do(schedule_jobs)

    while True:
        if has_pending_jobs(schedule.get_jobs()):
            logger.information('Starting jobs', 'Main')

            oracle_cnx.create_connection()
            oracle_cnx.create_cursor()

            schedule.run_pending()

            oracle_cnx.close_cursor()
            oracle_cnx.close_connection()

            logger.information('Jobs done', 'Main')
        else:
            print(f'No jobs at: {datetime.datetime.now().strftime("%H:%M")}')

        # if not empty
        if not update_eventual():
            logger.information('Starting eventual jobs', 'Eventual')

            for jobs_index, jobs in eventual.iterrows():
                oracle_cnx.create_connection()
                oracle_cnx.create_cursor()

                eventual_job(jobs)

                oracle_cnx.close_cursor()
                oracle_cnx.close_connection()

            logger.information('Eventual jobs finished', 'Eventual')

        try:
            time.sleep(120)
        except KeyboardInterrupt:
            exit(0)
