# DQ Rule 2 - Comparacion de tuplas 
# Version 4.0 - 06/04/2025 - OS

from modules.db_handler import *
from modules.ontology_handler import *
from modules.logger.shared_logger import *

def get_param_dq2_dq3_str_query(dq_app_method_id, dq_schema):
    return text(f"""
        WITH parent_child AS (
		SELECT 
			t1.*, 
			t2.dwhier_id,t2.dwhier_name, 
			t3.dwcat_id, t3.dwcat_position,
	  		t4.dwcat_name, t4.dwcat_table_attribute,
			t5.dwatt_id, t5.dwatt_column,
			t6.map_id, t6.dataprop_id, 
			t7.appmethod_id,
			t8.dataprop_url, t8.dataprop_domain_id, t8.dataprop_range_id,
			t9.class_id, t9.class_url,
			t10.datatype_name
	    FROM {dq_schema}.dw_dimension						t1
	    	INNER JOIN {dq_schema}.dw_hierarchy				t2 ON t1.dwdim_id = t2.dwdim_id
			INNER JOIN {dq_schema}.dw_category_position	    t3 ON t2.dwdim_id = t3.dwdim_id AND t2.dwhier_id = t3.dwhier_id
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
        t2.dwdim_name,
		t2.dwdim_table,
        -- Object Property
        t4.objectprop_url,
		-- Parent
		t2.dwatt_column		AS parent_dwatt_column, 
		t2.class_url		AS parent_class_url,
		t2.dataprop_url		AS parent_dataprop_url, 
		t2.datatype_name	AS parent_datatype_name,
		-- Child
		t3.dwatt_column		AS child_dwatt_column, 
		t3.class_url		AS child_class_url,
		t3.dataprop_url		AS child_dataprop_url, 
		t3.datatype_name	AS child_datatype_name
		
	FROM {dq_schema}.map_hier 					t1
		INNER JOIN parent_child			    	t2 ON t1.dwcat_parent_id = t2.dwcat_id
		INNER JOIN parent_child			    	t3 ON t1.dwcat_child_id = t3.dwcat_id
		INNER JOIN {dq_schema}.onto_object_property	t4 ON t1.objprop_id = t4.objectprop_id 
													AND t2.class_id = t4.range_id	-- Just a double check
													AND t3.class_id = t4.domain_id	-- Just a double check
    """)


def dq2_get_parameters_query(dq_app_method_id):
    # --- Database connection setup ---
    dq_session = get_DBsession()
    dq_schema = get_dq_schema()
      
    # --- Retrieve parameters for DQ2 syntactic accuracy rule from the DQ database ---
    # Version 1 for DQ_v5
    dq2_get_parameters_query = get_param_dq2_dq3_str_query(dq_app_method_id, dq_schema)

    log_verbose(dq2_get_parameters_query)
        
    try:
        result = dq_session.execute(dq2_get_parameters_query)
        dq2_parameters_list = [dict(row._mapping) for row in result]
        log_result(f"Parameters retrieved for DQ2 Semantic Accuracy and DQ3 Completeness rules.\n{dq2_parameters_list}")
        dq_session.close()
    except Exception as e:
        log_error(f"Error retrieving mapping information:  {e}")
        dq_session.close()
        raise
    if len(dq2_parameters_list) != 1:
        log_error("Error one row was expected getting dq query parameters.")
        raise
    
    #dq2_params tuple: ( dwdim_name, dwdim_table, objectprop_url,
    #                       parent_dwatt_column, parent_class_url,parent_dataprop_url, parent_datatype_name,
    #                       child_dwatt_column, child_class_url, child_dataprop_url, child_datatype_name )
    dq2_params = dict_to_namedtuple("DQParams", dq2_parameters_list[0])

    return dq2_params

def get_ontology_data(child_class_url, child_dataprop_url, parent_dataprop_url, objectprop_url):
    # --- Get the ontology data using SPARQL ---
    onto = load_ontology()
    
    # --- Query the ontology ---
    sparql_query = f"""
        SELECT ?domain_dp ?range_dp WHERE {{
            ?domain_class a <{child_class_url}> .
            ?domain_class <{child_dataprop_url}> ?domain_dp .
            ?domain_class <{objectprop_url}> ?range_class .
            ?range_class <{parent_dataprop_url}> ?range_dp .
        }}
        ORDER BY ?domain_dp
    """
    # Execute the query
    with onto:
        onto_query = list(default_world.sparql(sparql_query))
    log_result(f"\n---------------\nOntology Query Result\n---------------\n{onto_query}")
    return onto_query

def dq_semantic_accuracy(dq_app_method_id):  #dq_app_method_id shall be used to select from mapping tables
    log_milestone(f"Starting DQ2 Semantic Accuracy Rule for - {dq_app_method_id}")
    t = dq2_get_parameters_query(dq_app_method_id)
    ontology_data =  get_ontology_data(t.child_class_url, t.child_dataprop_url, t.parent_dataprop_url, t.objectprop_url)

    ####################################
    # Usando el resultado sparql, se ejecuta una consulta POSTGRESQL para semantic accuracy
    ####################################
    log_milestone("\n---------------\nSemantic Rule Result in one sql query\n---------------")    

    # --- Create a session with the data warehouse database ---
    dw_session = get_DBsession( get_dw_connection( dq_app_method_id ) )
    data_warehouse_schema = get_dw_schema()

    # y = objectprop_url(x)
    params = { f"tupla_{i}": (x, y) for i, (x, y) in enumerate(ontology_data) }
    values_clause = ", ".join([f":tupla_{i}" for i, _ in enumerate(ontology_data)])
   
    dq2_semantic_accuracy_query = text(f"""
            WITH tableFromOnto AS (
                SELECT * FROM  (VALUES {values_clause}) AS temp_table (domain, range)
            )
            SELECT DISTINCT
                t1.{t.child_dwatt_column},
                t1.{t.parent_dwatt_column},
                CASE
                    WHEN NOT EXISTS (
                            SELECT 1 FROM tableFromOnto t2 
                            WHERE 
                                t2.domain = cast(t1.{t.child_dwatt_column} AS Text)
                            ) THEN 2
                    WHEN EXISTS (
                            SELECT 1 FROM tableFromOnto t3  
                            WHERE 
                                t3.domain = cast(t1.{t.child_dwatt_column} AS Text) AND t3.range = t1.{t.parent_dwatt_column}
                            ) THEN 1
                    ELSE 0
                END AS boolean_flag,
                CASE
                    WHEN NOT EXISTS (
                            SELECT 1 FROM tableFromOnto t2 
                            WHERE 
                                t2.domain = cast(t1.{t.child_dwatt_column} AS Text)
                            ) THEN NULL
                    WHEN EXISTS (
                            SELECT 1 FROM tableFromOnto t3  
                            WHERE 
                                t3.domain = cast(t1.{t.child_dwatt_column} AS Text) AND t3.range = t1.{t.parent_dwatt_column}
                            ) THEN t1.{t.parent_dwatt_column}
                    ELSE (
                        SELECT STRING_AGG(t4.range::text, ' / ')
                        FROM tableFromOnto AS t4
                        WHERE t4.domain = cast(t1.{t.child_dwatt_column} AS Text) 
                    )
                END AS list_of_parents
                FROM {data_warehouse_schema}.{t.dwdim_table} t1
                WHERE t1.{t.child_dwatt_column} IS NOT NULL
                ORDER BY t1.{t.child_dwatt_column}
            """
            )
    
    try:
        result = dw_session.execute(dq2_semantic_accuracy_query , params)
        #### REVISAR OS
        #### Esto es para imprimir. SI lo quiero dejar dentro de log, deberia parametrizar y ver que el format se haga ahi.
        dq2_result = [dict(row._mapping) for row in result]
        tuple_result = [tuple(d.values()) for d in dq2_result]
        formatted_output = '\n'.join(str(t) for t in tuple_result)
        log_result(f"Semantic accuracy:\n{formatted_output}")
        dw_session.close()
    except Exception as e:
        log_error(f"Error executing semantic accuracy rule: {e}")
        dw_session.close()
        raise
