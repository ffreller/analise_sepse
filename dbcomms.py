# Usuario e senha
def get_usuario_senha_db_tns(db_tns):
    from credentials import USUARIO_PROD, SENHA_PROD, USUARIO_TESTE, SENHA_TESTE
    from base64 import b64decode
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
    return usuario, senha, vDB_TNS


# Cria engine do sqlalchemy
def create_sqlalchemy_engine(db_tns):
    from sqlalchemy import create_engine
    usuario, senha, vDB_TNS = get_usuario_senha_db_tns(db_tns)
    conn = create_engine(f'oracle+cx_oracle://{usuario}:{senha}@{vDB_TNS}')
    return conn


#Cria conexão via cxOracle
def create_conn_cxOracle(db_tns):
    from cx_Oracle import connect
    usuario, senha, vDB_TNS = get_usuario_senha_db_tns(db_tns)
    conn = connect(user=usuario, password=senha, dsn=vDB_TNS)
    return conn


# Executa query via pandas e sqlalchemy
def execute_query_pandas(query, conn):
    from pandas import read_sql
    return read_sql(query, conn)


# Executa query via cxOracle e carrega dados para dataframe
def execute_query_cxOracle_and_load_to_df(query, conn, columns):
    from pandas import DataFrame
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
    return DataFrame(rows, columns=columns)


# Lê queries presentes em arquivo .sql e separadas por comentários
def read_queries_from_file(fpath=None):
    from re import findall, split
    from src.definitions import MAIN_DIR
    if not fpath:
        fpath = MAIN_DIR/'data/sql_queries_sepse.sql'
    with open(fpath, 'r') as f:
        sqlFile = f.read()
    query_names = findall('-- (.*)', sqlFile)
    queries = split('--.*', sqlFile)[1:]
    return dict(zip(query_names, queries))


# Script para baixar dados do HAOC_TASY_PROD
def retrieve_data_from_dbtasy_using_dates(start_date, end_date):
    from logging import getLogger
    from src.definitions import RAW_DATA_DIR
    
    logger = getLogger('standard')
    error_logger = getLogger('error')
    
    logger.debug("Baixando dados do DB_TASY: De %s até %s" % (start_date, end_date))
    queries = read_queries_from_file()
    sqlalchemy_engine = create_sqlalchemy_engine('tasy')
    conn_cxOracle = create_conn_cxOracle('tasy')
    success = True
    for query_name in queries.keys():
        query = queries[query_name]
        assert 'DATE_TO_REPLACE_START' in query, "Sem data início para substituir"
        assert 'DATE_TO_REPLACE_END' in query, "Sem data fim para substituir"
        query = query.replace('DATE_TO_REPLACE_START', start_date).replace('DATE_TO_REPLACE_END', end_date)
        try:
            if 'evolução' not in query_name.lower():
                df = execute_query_pandas(query, sqlalchemy_engine)
                df.columns = [col.upper() for col in df.columns]
            else:
                df = execute_query_cxOracle_and_load_to_df(query, conn_cxOracle, 
                                                           columns=["CD_ESTABELECIMENTO","NR_ATENDIMENTO","DT_EVOLUCAO","DT_LIBERACAO","DS_EVOLUCAO"])
        except Exception as e:
            logger.error('Erro ao excecutar query %s: %s' % (query_name.title(), str(e)))
            error_logger.error('Erro ao excecutar query %s: %s' % (query_name.title(), str(e)))
            success = False
        if success:
            logger.debug("Query '%s' baixada com sucesso" % query_name.title())
            df.to_pickle(RAW_DATA_DIR/f"{query_name.title().replace(' ', '_')}.pickle")
    sqlalchemy_engine.dispose()
    conn_cxOracle.close()
    return success


# Script para baixar dados do mês passado HAOC_TASY_PROD
def retrieve_last_month_data_from_dbtasy():
    from datetime import datetime
    from src.helper_functions import get_last_month_and_year
    target_month, target_year, next_month, year_of_next_month = get_last_month_and_year()
    first_day_of_target_month = datetime(year=target_year, month=target_month, day=1)
    first_day_of_next_month = datetime(year=year_of_next_month, month=next_month, day=1)
    start = first_day_of_target_month.strftime('%d/%m/%Y')
    end = first_day_of_next_month.strftime('%d/%m/%Y')
    return retrieve_data_from_dbtasy_using_dates(start, end)