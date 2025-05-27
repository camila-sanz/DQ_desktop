from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from collections import namedtuple
from modules.logger.shared_logger import *


DQ_DATABASE_URI = "postgresql://postgres:postgres@localhost:5432/dq_desktop_qdb25" 

def get_DBsession(db_uri = DQ_DATABASE_URI):
    """ Crea y devuelve una sesión de base de datos """
    engine = create_engine(db_uri)
    dqSession = sessionmaker(bind=engine)
    return dqSession()

def get_dw_schema():
    return "public"

def get_dq_schema():
    return "public"

def dict_to_namedtuple(name, d):
    return namedtuple(name, d.keys())(**d)

def get_dw_connection(dq_app_method_id):
    dq_session = get_DBsession()
    get_dw_connection_query = text("SELECT dw_connection FROM dw_datawarehouse")
    
    try:
        result = dq_session.execute(get_dw_connection_query)
        data_warehouse_connection = result.fetchone()[0]
    except Exception as e:
        log_error(f"Error retrieving dw connection: {e}")
        dq_session.close()
        raise
        
    log_result(f"Data warehouse conection: {data_warehouse_connection}")
    dq_session.close()
    return data_warehouse_connection





