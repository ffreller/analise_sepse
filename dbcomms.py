
import pandas as pd
from base64 import b64decode
import re
import cx_Oracle
from datetime import datetime
from sqlalchemy import create_engine
from src.helper_functions import print_with_time, get_last_month_and_year
from src.definitions import RAW_DATA_DIR
from credentials import USUARIO_PROD, SENHA_PROD, USUARIO_TESTE, SENHA_TESTE

def create_conn_sqlalchemy(db_tns):
    if db_tns.lower() == 'odi':
        usuario = b64decode(USUARIO_PROD).decode("utf-8")
        senha = b64decode(SENHA_PROD).decode("utf-8")
        vDB_TNS = "DB_ODI_PROD"
    elif db_tns.lower() == 'test':
        usuario = b64decode(USUARIO_TESTE).decode("utf-8")
        senha = b64decode(SENHA_TESTE).decode("utf-8")
        vDB_TNS = "DBTESTE1"
    elif db_tns.lower() == 'tasy':
        usuario = b64decode(USUARIO_PROD).decode("utf-8")
        senha = b64decode(SENHA_PROD).decode("utf-8")
        vDB_TNS = "HAOC_TASY_PROD"
    else:
        print("O argumento db_tns precisa ser igual a 'test', 'tasy' ou 'odi'")
        return None
    conn = create_engine(f'oracle+cx_oracle://{usuario}:{senha}@{vDB_TNS}')
    return conn


# Executa query via cx_Oracle
def create_conn_cxOracle(db_tns):
    if db_tns.lower() == 'odi':
        usuario = b64decode(USUARIO_PROD).decode("utf-8")
        senha = b64decode(SENHA_PROD).decode("utf-8")
        vDB_TNS = "DB_ODI_PROD"
    elif db_tns.lower() == 'test':
        usuario = b64decode(USUARIO_TESTE).decode("utf-8")
        senha = b64decode(SENHA_TESTE).decode("utf-8")
        vDB_TNS = "DBTESTE1"
    elif db_tns.lower() == 'tasy':
        usuario = b64decode(USUARIO_PROD).decode("utf-8")
        senha = b64decode(SENHA_PROD).decode("utf-8")
        vDB_TNS = "HAOC_TASY_PROD"
    conn = cx_Oracle.connect(user=usuario, password=senha, dsn=vDB_TNS)
    return conn


# Executa query via pandas
def execute_query_cxOracle_and_load_to_pandas(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=["CD_ESTABELECIMENTO","NR_ATENDIMENTO","DT_EVOLUCAO","DT_LIBERACAO","DS_EVOLUCAO"])
    return df


# Executa query via pandas
def execute_query_pandas(query, conn):
    df = pd.read_sql(query, conn)
    return df


def read_queries_from_file(fpath=None):
    if not fpath:
        fpath = 'data/sql_queries_sepse.sql'
    with open(fpath, 'r') as f:
        sqlFile = f.read()
    query_names = re.findall('-- (.*)', sqlFile)
    queries = re.split('--.*', sqlFile)[1:]
    dict_queries = dict(zip(query_names, queries))
    return dict_queries


# Script para baixar dados do HAOC_TASY_PROD
def retrieve_data_from_dbtasy_using_dates(start_date, end_date):
    print_with_time(f"Baixando dados do DB_TASY: De {start_date} até {end_date}")
    queries = read_queries_from_file()
    conn_sqlalchemy = create_conn_sqlalchemy('tasy')
    conn_cxOracle = create_conn_cxOracle('tasy')
    success = True
    for query_name in queries.keys():
        query = queries[query_name]
        query = query.replace('DATE_TO_REPLACE_START', start_date).replace('DATE_TO_REPLACE_END', end_date)
        if 'evolução' not in query_name.lower():
            try:
                df = execute_query_pandas(query, conn_sqlalchemy)
                # assert len(df) > 0, print(f'Erro ao baixar dados query {query_name.upper()}: dataframe vazio')
                df.columns = [col.upper() for col in df.columns]
            except Exception as e:
                print_with_time(f'Erro ao excecutar query {query_name.title()}: ' + str(e))
                success = False
        else:
            try:
                df = execute_query_cxOracle_and_load_to_pandas(query, conn_cxOracle)
            except Exception as e:
                print_with_time(f'Erro ao excecutar query {query_name.title()}: ' + str(e))
                success = False
        if success:
            print_with_time(f"Query '{query_name.title()}' baixada com sucesso")
            df.to_pickle(RAW_DATA_DIR/f"{query_name.title().replace(' ', '_')}.pickle")
    conn_sqlalchemy.dispose()
    conn_cxOracle.close()
    return success


# Script para baixar dados do mês passado HAOC_TASY_PROD
def retrieve_last_month_data_from_dbtasy():
    target_month, target_year, next_month, year_of_next_month = get_last_month_and_year()
    first_day_of_target_month = datetime(year=target_year, month=target_month, day=1)
    first_day_of_next_month = datetime(year=year_of_next_month, month=next_month, day=1)
    start = first_day_of_target_month.strftime('%d/%m/%Y')
    end = first_day_of_next_month.strftime('%d/%m/%Y')
    return retrieve_data_from_dbtasy_using_dates(start, end)