
import pandas as pd
from base64 import b64decode
import re
import cx_Oracle
from sqlalchemy import create_engine
from src.helper_functions import print_with_time, get_first_and_last_day_of_month
from src.definitions import RAW_DATA_DIR
from credentials import USUARIO_PROD, SENHA_PROD, USUARIO_TESTE, SENHA_TESTE

def create_db_conn(bd_tns):
    if bd_tns.lower() == 'odi':
        usuario = b64decode(USUARIO_PROD).decode("utf-8")
        senha = b64decode(SENHA_PROD).decode("utf-8")
        vDB_TNS = "DB_ODI_PROD"
    elif bd_tns.lower() == 'test':
        usuario = b64decode(USUARIO_TESTE).decode("utf-8")
        senha = b64decode(SENHA_TESTE).decode("utf-8")
        vDB_TNS = "DBTESTE1"
    elif bd_tns.lower() == 'prod':
        usuario = b64decode(USUARIO_PROD).decode("utf-8")
        senha = b64decode(SENHA_PROD).decode("utf-8")
        vDB_TNS = "HAOC_TASY_PROD"
    else:
        print("O argumento test_prod precisa ser igual a 'test', 'prod' ou 'odi'")
        return None
    conn = create_engine(f'oracle+cx_oracle://{usuario}:{senha}@{vDB_TNS}')
    return conn


def read_queries_from_file(fpath=None):
    if not fpath:
        fpath = 'data/sql_queries.sql'
    with open(fpath, 'r') as f:
        sqlFile = f.read()
    query_names = re.findall('-- (.*)', sqlFile)
    queries = re.split('--.*', sqlFile)[1:]
    dict_queries = dict(zip(query_names, queries))
    return dict_queries


# Script para baixar dados do mês passado HAOC_TASY_PROD
def retrieve_last_month_data_from_dbprod():
    first_day_of_month, last_day_of_month = get_first_and_last_day_of_month()
    start = first_day_of_month.strftime('%d/%m/%Y')
    end = last_day_of_month.strftime('%d/%m/%Y')
    return retrieve_data_from_dbprod_using_dates(start, end)


# Executa query via cx_Oracle
def execute_query_cxOracle(query):
    usuario = b64decode(USUARIO_PROD).decode("utf-8")
    senha = b64decode(SENHA_PROD).decode("utf-8")
    vDB_TNS = "HAOC_TASY_PROD"
    connection = cx_Oracle.connect(user=usuario, password=senha, dsn=vDB_TNS)
    cur = connection.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["CD_ESTABELECIMENTO","NR_ATENDIMENTO","DT_EVOLUCAO","DT_LIBERACAO","DS_EVOLUCAO"])
    return df


# Executa query via pandas
def execute_query_pandas(query, conn):
    df = pd.read_sql(query, conn)
    return df


# Script para baixar dados do HAOC_TASY_PROD
def retrieve_data_from_dbprod_using_dates(start_date, end_date):
    print_with_time(f"Baixando dados do DB_OD_PROD\nDe {start_date} até {end_date}")
    queries = read_queries_from_file()
    conn = create_db_conn('prod')
    success = True
    for query_name in queries.keys():
        query = queries[query_name]
        query = query.replace('DATE_TO_REPLACE_START', start_date).replace('DATE_TO_REPLACE_END', end_date)
        if 'evolução' not in query_name.lower():
            try:
                df = execute_query_pandas(query, conn)
                # assert len(df) > 0, print(f'Erro ao baixar dados query {query_name.upper()}: dataframe vazio')
                df.columns = [col.upper() for col in df.columns]
            except Exception as e:
                print_with_time(f'Erro ao excecutar query {query_name.title()}: ' + str(e))
                success = False
        else:
            try:
                df = execute_query_cxOracle(query)
            except Exception as e:
                print_with_time(f'Erro ao excecutar query {query_name.title()}: ' + str(e))
                success = False
        if success:
            print_with_time(f"Query '{query_name.title()}' baixada com sucesso")
            df.to_pickle(RAW_DATA_DIR/f"{query_name.title().replace(' ', '_')}.pickle")
    return success