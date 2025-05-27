# DQ Rule 1 - Comparacion nombres de TODAS las entradas de Mapping_View!
# Version 6.0 - 06/04/2025 - OS/LA
# Se probó solo con strings. 

from modules.db_handler import *
from modules.ontology_handler import *
from modules.logger.shared_logger import *

def dq1_get_parameters_query(dq_app_method_id):
    # --- Database connection setup ---
    dq_session = get_DBsession()
    dq_schema = get_dq_schema()
        
    # --- Retrieve parameters for DQ1 syntactic accuracy rule from the DQ database ---
    # Version 1 for DQ_v5
    dq1_get_parameters_query = text(f"""
        WITH allFields AS ( 
	        SELECT 
		        t1.*, 
		        t2.dwhier_id,t2.dwhier_name, 
		        t3.dwcat_id, t3.dwcat_position,
  		        t4.dwcat_name, t4.dwcat_table_attribute,
		        t5.dwatt_id, t5.dwatt_column,
		        t6.map_id, t6.dataprop_id, 
		        t7.appmethod_id,
		        t8.dataprop_url, t8.dataprop_domain_id, t8.dataprop_range_id,
		        t9.class_url,
		        t10.datatype_name
            FROM {dq_schema}.dw_dimension						t1
    	        INNER JOIN {dq_schema}.dw_hierarchy				t2 ON t1.dwdim_id = t2.dwdim_id
		        INNER JOIN {dq_schema}.dw_category_position     t3 ON t2.dwdim_id = t3.dwdim_id AND t2.dwhier_id = t3.dwhier_id
		        INNER JOIN {dq_schema}.dw_category				t4 ON t3.dwcat_id = t4.dwcat_id
		        INNER JOIN {dq_schema}.dw_attribute				t5 ON t4.dwcat_id = t5.dwcat_id
		        INNER JOIN {dq_schema}.map_att					t6 ON t5.dwcat_id = t6.dwcat_id AND t5.dwatt_id = t6.dwatt_id
		        INNER JOIN {dq_schema}.dq_appmethod_map_assoc	t7 ON t6.map_id = t7.map_id
		        INNER JOIN {dq_schema}.onto_data_property		t8 ON t6.dataprop_id = t8.dataprop_id
		        INNER JOIN {dq_schema}.onto_class 				t9 ON t8.dataprop_domain_id = t9.class_id
		        INNER JOIN {dq_schema}.onto_data_type 			t10 ON t8.dataprop_range_id = t10.datatype_id
	        WHERE t7.appmethod_id = {dq_app_method_id}
        )
   
        SELECT 
	        dwdim_name,
	        dwdim_table,
	        dwatt_column, 
	        class_url,
	        dataprop_url, 
	        datatype_name
        FROM allFields
        """)
    log_verbose(dq1_get_parameters_query)
        
    try:
        result = dq_session.execute(dq1_get_parameters_query)
        dq1_parameters_list = [dict(row._mapping) for row in result]
        log_result(f"Parameters retrieved for DQ1 Syntactic Accuracy Rule.\n{dq1_parameters_list}")
        dq_session.close()
    except Exception as e:
        log_error(f"Error retrieving mapping information:  {e}")
        dq_session.close()
        raise
    if len(dq1_parameters_list) != 1:
        log_error("Error one row was expected getting dq query parameters.")
        raise
    
    #dq1_params tuple(dwdim_name,dwdim_table, dwatt_column, class_url, dataprop_url, datatype_name)
    dq1_params = dict_to_namedtuple("DQParams", dq1_parameters_list[0])

    return dq1_params

####################################
# Get data from the ontology using SPARQL
####################################
def get_ontology_data(class_url, dataprop_url):
     # --- Load Ontology using owlready2 ---
    onto = load_ontology()

    # Como esta hecha queda con None, si queremos  NULL se puede poner:       
    #       (IF(BOUND(?range_class), ?range_dp, "NULL") AS ?range_dp2) 
    sparql_query = f"""
        SELECT DISTINCT
            ?onto_class_dp 
        WHERE {{
            ?onto_class a <{class_url}> .
            ?onto_class <{dataprop_url}> ?onto_class_dp .
        }}
    """
    with onto:
        onto_query = list(default_world.sparql(sparql_query))
    log_result(f"""---------------\n
                   Query en la Ontologia\n
                   ---------------\n
                   {onto_query}
              """)
    return onto_query

def dq_syntactic_accuracy(dq_app_method_id):  #dq_app_method_id shall be used to select from mapping tables
    log_milestone(f"Starting DQ1 Syntactic Accuracy Rule for - {dq_app_method_id}")
    t = dq1_get_parameters_query(dq_app_method_id)
    ontology_data =  get_ontology_data(t.class_url, t.dataprop_url)

    ####################################
    # Usando el resultado sparql, se ejecuta una consulta POSTGRESQL para syntactic accuracy
    ####################################
    log_milestone("---------------\nSyntactic Accuracy Rule \n---------------")
        
    # --- Create a session with the data warehouse database ---
    dw_session = get_DBsession( get_dw_connection( dq_app_method_id ) )
    data_warehouse_schema = get_dw_schema()

    params = { f"tupla_{i}": tuple(onto_class_dp) for i, (onto_class_dp) in enumerate(ontology_data) }
    values_clause = ", ".join([f":tupla_{i}" for i, _ in enumerate(ontology_data )])

    dq1_syntactic_accuracy_query = text(f"""
            WITH tableFromOnto AS (
                SELECT * FROM  (VALUES {values_clause}) AS temp_table (class_dp)
            )
            SELECT DISTINCT
                t1.{t.dwatt_column}, 
                CASE 
                    WHEN t1.{t.dwatt_column} = t2.class_dp THEN 1
                    ELSE 0
                END AS Flag
            FROM {data_warehouse_schema}.{t.dwdim_table} t1
            LEFT JOIN  tableFromOnto t2 ON t1.{t.dwatt_column} = t2.class_dp
            WHERE t1.{t.dwatt_column} IS NOT NULL
            """
            )
       
    try:
        result = dw_session.execute(dq1_syntactic_accuracy_query , params)
        #### REVISAR OS
        #### Esto es para imprimir. SI lo quiero dejar dentro de log, deberia parametrizar y ver que el format se haga ahi.
        dq1_result = [dict(row._mapping) for row in result]
        tuple_result = [tuple(d.values()) for d in dq1_result]
        formatted_output = '\n'.join(str(t) for t in tuple_result)
        log_result(f"Syntactic accuracy:\n{formatted_output}")
        #REVISAR OS, seguramente deberiamos retornar el resultado. Consultar con CAMI.
        dw_session.close()
    except Exception as e:
        log_error(f"Error executing syntactic accuracy rule: {e}")
        dw_session.close()
        raise


