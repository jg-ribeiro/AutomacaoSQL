import os
import json
import sqlite3
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

CONFIG_FILE = 'datafile.json'

def load_config():
    """Carrega os parâmetros de conexão do arquivo JSON."""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Erro: Arquivo de configuração '{CONFIG_FILE}' não encontrado.")
        exit(1)

def get_postgres_engine(params):
    """Cria e retorna um engine SQLAlchemy para o PostgreSQL."""
    pg_params = params['postgres']
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

def migrate_table(sqlite_cursor, pg_conn, sqlite_table, pg_table, column_map=None):
    """
    Função genérica para migrar dados de uma tabela SQLite para uma tabela PostgreSQL.
    
    :param sqlite_cursor: Cursor da conexão SQLite.
    :param pg_conn: Conexão ativa do SQLAlchemy para o PostgreSQL.
    :param sqlite_table: Nome da tabela de origem no SQLite.
    :param pg_table: Nome da tabela de destino no PostgreSQL.
    :param column_map: Dicionário para mapear nomes de colunas se forem diferentes.
    """
    print(f"\nIniciando migração da tabela '{sqlite_table}' para '{pg_table}'...")
    
    # 1. Ler dados do SQLite
    sqlite_cursor.execute(f"SELECT * FROM {sqlite_table} ")
    rows = sqlite_cursor.fetchall()
    
    if not rows:
        print(f"Tabela '{sqlite_table}' está vazia. Nenhuma migração necessária.")
        return

    # 2. Preparar para inserção no PostgreSQL
    # Pega os nomes das colunas da primeira linha (usando sqlite3.Row)
    sqlite_columns = list(rows[0].keys())
    
    # Mapeia colunas se necessário
    pg_columns = [column_map.get(col, col) for col in sqlite_columns] if column_map else sqlite_columns

    columns_str = ", ".join(f'"{c}"' for c in pg_columns) # Aspas duplas para segurança
    placeholders = ", ".join([f":{col}" for col in sqlite_columns])

    # 3. Executar a inserção
    insert_sql = text(f"INSERT INTO {pg_table} ({columns_str}) VALUES ({placeholders})")
    
    # Converte sqlite3.Row para uma lista de dicionários
    data_to_insert = [dict(row) for row in rows]
    
    pg_conn.execute(insert_sql, data_to_insert)
    print(f"Sucesso! {len(data_to_insert)} linhas migradas para a tabela '{pg_table}'.")

def update_postgres_sequences(pg_conn, tables_with_serial):
    """
    Atualiza o valor das sequências no PostgreSQL após a inserção de dados com IDs explícitos.
    """
    print("\nAtualizando sequências do PostgreSQL...")
    for table, pk_column in tables_with_serial:
        try:
            # Pega o nome da sequência associada à coluna SERIAL
            seq_name_query = text(f"SELECT pg_get_serial_sequence('{table}', '{pk_column}')")
            seq_name_result = pg_conn.execute(seq_name_query).scalar_one_or_none()

            if seq_name_result:
                # Atualiza o valor da sequência para o maior ID atual + 1
                update_seq_query = text(f"SELECT setval('{seq_name_result}', COALESCE((SELECT MAX({pk_column}) FROM {table}), 1))")
                pg_conn.execute(update_seq_query)
                print(f"  - Sequência para '{table}.{pk_column}' atualizada.")
            else:
                print(f"  - Aviso: Não foi possível encontrar a sequência para '{table}.{pk_column}'.")

        except Exception as e:
            print(f"  - Erro ao atualizar a sequência para '{table}': {e}")


def main():
    """Função principal para orquestrar a migração."""
    params = load_config()
    
    # Conexão com PostgreSQL
    pg_engine = get_postgres_engine(params)
    
    # Conexão com SQLite
    sqlite_db_path = params['backend']['db_path']
    if not os.path.exists(sqlite_db_path):
        print(f"Erro: O arquivo de banco de dados SQLite '{sqlite_db_path}' não foi encontrado.")
        return
        
    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_conn.row_factory = sqlite3.Row # Permite acessar colunas por nome
    sqlite_cursor = sqlite_conn.cursor()
    
    # Ordem de migração (respeitando dependências de chaves estrangeiras)
    # Formato: (tabela_sqlite, tabela_postgres, mapa_de_colunas_opcional)
    migration_order = [
        ('parameters', 'parameters', None),
        ('users', 'users', None),
        # Lendo da tabela 'jobs_he_new' (conforme seu script) e inserindo em 'jobs_he'
        ('jobs_he', 'jobs_he', None), 
        ('jobs_de', 'jobs_de', None)
    ]

    # Lista de tabelas e suas chaves primárias seriais para atualizar as sequências
    tables_with_serial_pks = [
        ('parameters', 'parameter_id'),
        ('users', 'user_id'),
        ('jobs_he', 'job_id'),
        ('jobs_de', 'schedule_id')
    ]

    # Usar uma transação para garantir a integridade dos dados
    with pg_engine.begin() as pg_conn:
        print("-" * 50)
        print("Iniciando migração de dados em uma transação...")
        
        try:
            for sqlite_tbl, pg_tbl, col_map in migration_order:
                migrate_table(sqlite_cursor, pg_conn, sqlite_tbl, pg_tbl, col_map)
            
            update_postgres_sequences(pg_conn, tables_with_serial_pks)
            
            print("\nTRANSAÇÃO CONCLUÍDA! Os dados serão comitados.")

        except Exception as e:
            print(f"\nERRO DURANTE A MIGRAÇÃO: {e}")
            print("A TRANSAÇÃO SERÁ REVERTIDA (ROLLBACK). Nenhuma alteração foi salva no PostgreSQL.")
            # O 'with' block cuidará do rollback automaticamente ao sair com uma exceção
            raise
    
    print("-" * 50)
    print("Migração finalizada.")

    # Fechar conexão com SQLite
    sqlite_conn.close()


if __name__ == '__main__':
    main()