# DQ Rule 3 - Completeness 
# 
# Version 3.0 - 06/04/2025 - OS
# Buscamos las tuplas que no tienen definido un padre en una relacion de jerarquia, ej. un libro no tiene genero.
# Usamos la misma consulta que en DQ2, de la consulta obtenemos todo lo necesario para aplicar la regla.
# retorna:
#    0 - si no esta en la dw y esta en la ontologia 
#    1 - si esta en la dw y esta en la ontologia 
#        si no esta en la dw y no esta en la ontologia 
#    2 - si esta en la dw y no esta en la ontologia 

from modules.db_handler import *
from modules.ontology_handler import *
from modules.logger.shared_logger import *
#REVISAR OS si terminamos con DQ_common o DQ_services para factorizar mas cosas. Esta iria ahi.
from modules.dq_rules.DQ2_semantic_accuracy import dq2_get_parameters_query   

def get_ontology_data(child_class_url, child_dataprop_url, parent_dataprop_url, objectprop_url):
    # --- Get the ontology data using SPARQL ---
    onto = load_ontology()
    log_milestone(f"Ontology loaded: {onto}")
    # --- Query the ontology ---
    sparql_query = f"""
    SELECT
        ?domain_dp 
        (BOUND(?range_class) AS ?range_class_exist)
        ?range_dp
    WHERE {{
        ?domain_class a <{child_class_url}> .
        ?domain_class <{child_dataprop_url}> ?domain_dp .
        OPTIONAL {{
            ?domain_class <{objectprop_url}> ?range_class .
            ?range_class <{parent_dataprop_url}> ?range_dp .
        }}
    }}
    """
    log_milestone(f"SPARQL Query: {sparql_query}")
    # Execute the query
    with onto:
        onto_query = list(default_world.sparql(sparql_query))
    log_result(f"\n---------------\nOntology Query Result\n---------------\n{onto_query}")
    return onto_query

def dq_completeness(dq_app_method_id, grandparent_dwatt_column = None):
    if grandparent_dwatt_column is None: 
        log_milestone(f"Starting DQ3 Completeness Rule for - {dq_app_method_id} ")
        grandparent_dwatt_column = ""
        grandpa_where_condition = ""
    else:
        log_milestone(f"DQ3 Completeness Rule for IMPROVEMENT COMPLETENESS- {dq_app_method_id} - {grandparent_dwatt_column} ")
        grandparent_dwatt_column += ","
        grandpa_where_condition = "WHERE Flag = 0"

    t = dq2_get_parameters_query(dq_app_method_id)
    ontology_data =  get_ontology_data(t.child_class_url, t.child_dataprop_url, t.parent_dataprop_url, t.objectprop_url)
    
    ####################################
    # Usando el resultado sparql, se ejecuta una consulta POSTGRESQL para semantic accuracy
    ####################################
    log_milestone("\n---------------\nCompleteness Rule Result in one sql query\n---------------")
    # --- Create a session with the data warehouse database ---
    dw_session = get_DBsession( get_dw_connection( dq_app_method_id ) )
    data_warehouse_schema = get_dw_schema()

    # (x, exist(y), y) where y  = objectprop_url(x)
    params = { f"tupla_{i}": (b_dom,b_op, b_range) for i, (b_dom,b_op, b_range) in enumerate(ontology_data) }
    values_clause = ", ".join([f":tupla_{i}" for i, _ in enumerate(ontology_data)])
    
    dq3_completeness_query = text(f"""
        WITH tableFromOnto AS (
            SELECT * FROM  (VALUES {values_clause}) AS temp_table (domain, obj_op, range)
        ),
        tableResult AS (
            SELECT DISTINCT
                {grandparent_dwatt_column}      -- this column is only present if dq3 is executed for improvement
                t1.{t.child_dwatt_column},
                t1.{t.parent_dwatt_column},
                CASE
                    WHEN t1.{t.parent_dwatt_column} IS NULL AND t2.obj_op THEN 0
                    WHEN t2.obj_op IS NULL THEN 2  -- The child_field_name is not present in Ontology and the left join insert none
                    WHEN NOT ( t1.{t.parent_dwatt_column}  IS NULL) AND NOT t2.obj_op THEN 2
                    ELSE 1
                END AS Flag,
                CASE
                    WHEN t1.{t.parent_dwatt_column} IS NULL AND t2.obj_op THEN (
                                    SELECT STRING_AGG(t3.range::text, ' / ') 
                                    FROM tableFromOnto AS t3
                                    WHERE t3.domain = cast(t1.{t.child_dwatt_column} AS Text) 
                    )
                    WHEN t2.obj_op IS NULL THEN NULL  -- The child_field_name is not present in Ontology and the left join insert none
                    WHEN NOT ( t1.{t.parent_dwatt_column}  IS NULL) AND NOT t2.obj_op  THEN NULL
                    ELSE (
                            SELECT STRING_AGG(t4.range::text, ' / ') 
                            FROM tableFromOnto AS t4
                            WHERE t4.domain = cast(t1.{t.child_dwatt_column} AS Text)
                    )
                END AS list_of_parents
            FROM {data_warehouse_schema}.{t.dwdim_table} t1
            LEFT JOIN  tableFromOnto t2 ON cast(t1.{t.child_dwatt_column} AS Text) = t2.domain 
            WHERE t1.{t.child_dwatt_column} IS NOT NULL
            ORDER BY Flag 
        )
        SELECT * FROM tableResult {grandpa_where_condition}
        """
        )
    try:
        result = dw_session.execute(dq3_completeness_query , params)
        #### REVISAR OS
        #### Esto es para imprimir. SI lo quiero dejar dentro de log, deberia parametrizar y ver que el format se haga ahi.
        dq3_result = [dict(row._mapping) for row in result]
        # Remove the last attribute from each dictionary
        tuple_result = [tuple(list(d.values())[:-1]) for d in dq3_result]
        formatted_output = '\n'.join(str(t) for t in tuple_result)
        log_result(f"Completeness :\n{formatted_output}")
        dw_session.close()
    except Exception as e:
        log_error(f"Error executing completeness rule: {e}")
        dw_session.close()
        raise
    return tuple_result
