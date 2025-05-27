# DQ Rule 4 - Verifica unicidad de una dependencia en dw (ej. subgenero -> genero)
# Version 2.0 - 07/04/2025 - OS
# Solucion consulta 100 % SQL sobre el DW.

from modules.db_handler import *
from modules.ontology_handler import *
from modules.logger.shared_logger import *
#REVISAR OS si terminamos con DQ_common o DQ_services para factorizar mas cosas. Esta iria ahi.
from modules.dq_rules.DQ2_semantic_accuracy import dq2_get_parameters_query  

def get_param_dq4_str_query(dq_app_method_id, dq_schema):
    return text(f"""
    WITH attribute_info AS (
	    SELECT * FROM {dq_schema}.dw_dimension				t1
		    INNER JOIN {dq_schema}.dw_hierarchy				t2 ON t1.dwdim_id = t2.dwdim_id
		    INNER JOIN {dq_schema}.dw_category_position	    t3 ON t2.dwdim_id = t3.dwdim_id AND t2.dwhier_id = t3.dwhier_id
		    INNER JOIN {dq_schema}.dw_category				t4 ON t3.dwcat_id = t4.dwcat_id
		    INNER JOIN {dq_schema}.dw_attribute				t5 ON t4.dwcat_id = t5.dwcat_id
		    INNER JOIN {dq_schema}.dq_appmethod_dwatt		t6 ON t5.dwcat_id = t6.dwcat_id AND t5.dwatt_id = t6.dwatt_id
	    WHERE t6.dqappmethod_id = {dq_app_method_id}
		
	    )

    SELECT 
	    t1.dwdim_table,
	    t1.dwatt_column AS x,
	    t2.dwatt_column AS y
    FROM attribute_info 					t1
    INNER JOIN attribute_info				t2 ON t1.dqappmethod_id = t2.dqappmethod_id
    WHERE t1.priority = 1 AND t2.priority = 2
    """)
    
def dq4_get_parameters_query(dq_app_method_id):
    # --- Database connection setup ---
    dq_session = get_DBsession()
    dq_schema = get_dq_schema()
      
    # --- Retrieve parameters for DQ2 syntactic accuracy rule from the DQ database ---
    # Version 1 for DQ_v5
    dq4_get_parameters_query = get_param_dq4_str_query(dq_app_method_id, dq_schema)

    log_verbose(dq4_get_parameters_query)
        
    try:
        result = dq_session.execute(dq4_get_parameters_query)
        dq4_parameters_list = [dict(row._mapping) for row in result]
        log_result(f"Parameters retrieved for DQ4 Consistency rules.\n{dq4_parameters_list}")
        dq_session.close()
    except Exception as e:
        log_error(f"Error retrieving mapping information:  {e}")
        dq_session.close()
        raise
    if len(dq4_parameters_list) != 1:
        log_error("Error one row was expected getting dq query parameters.")
        raise
    
    #dq4_params tuple: ( dwdim_table, X, Y)
    dq4_params = dict_to_namedtuple("DQParams", dq4_parameters_list[0])

    return dq4_params

def dq_consistency(dq_app_method_id):  
    log_milestone(f"Starting DQ4 Consistency Rule for - {dq_app_method_id}")
    t = dq4_get_parameters_query(dq_app_method_id)
        
    log_milestone("\n---------------\nConsistency Rule Result in one sql query\n---------------")
    # --- Create a session with the data warehouse database ---
    dw_session = get_DBsession( get_dw_connection( dq_app_method_id ) )
    data_warehouse_schema = get_dw_schema()

    dq4_consistency_query = text(f"""
        SELECT 
            {t.x},
            CAST((COUNT(DISTINCT {t.y}) + SUM(CASE WHEN {t.y} IS NULL THEN 1 ELSE 0 END) < 2) AS INTEGER) AS flag,  
	        SUM(CASE WHEN {t.y} IS NULL THEN 1 ELSE 0 END) AS y_nulls_count, 
	        COUNT( DISTINCT {t.y} ) AS y_distinct_count, 
            STRING_AGG({t.y}, '/ ') AS y_list
        FROM {data_warehouse_schema}.{t.dwdim_table} t1
        GROUP BY {t.x};
        """)
    try:
        result = dw_session.execute(dq4_consistency_query)
        #### REVISAR OS
        #### Esto es para imprimir. SI lo quiero dejar dentro de log, deberia parametrizar y ver que el format se haga ahi.
        dq4_result = [dict(row._mapping) for row in result]
        tuple_result = [tuple(d.values()) for d in dq4_result]
        formatted_output = '\n'.join(str(t) for t in tuple_result)
        
        log_result(f"Consistency :\n{dq4_result[0].keys()}\n{formatted_output}")
        dw_session.close()
    except Exception as e:
        log_error(f"Error executing consistency rule: {e}")
        dw_session.close()
        raise
    return tuple_result
