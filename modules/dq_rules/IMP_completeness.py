# -*- coding: utf-8 -*-

# Completeness Improvement 
# 
# Version 1.0 for dq_v5 - 14/05/2025 - OS  REVISAR OS: Se precisa un nombre para versionar y numeros!!! 
#										   REVISAR OS: de hecho esta no es dq_v5 porque esta tabla CAMBIO! 	
# This improvement shall improve the possible set of values for completenss results.
# In order to improve the set of values, another relationship between attributes and ontology will be used.
# dq_improvement table has the data needed to execute the rule:
#		dqimprovement_id	: IMP1, this field was used to reach the routine.
#		dqimprovement_name	: Name of the rule
#		dqimprov_method_id	: used to select the row in the table where the parameters will be taken.
#		dqappliedmethod_id	: DQ applied method to be improved. It is a map_hier ID where the (child parent) info is obtained
#		maphier_id_parent_grandparent : Another maphier ID where the (parent grandparent) info is obtained
#		dqimprov_description: A brief general description e.g. "Completeness Improvement"

from sqlalchemy import tuple_
from modules.db_handler import *
from modules.dq_rules.DQ2_semantic_accuracy import dq2_get_parameters_query
from modules.ontology_handler import *
from modules.logger.shared_logger import *
from modules.dq_rules.DQ3_completeness import dq_completeness
from modules.dq_rules.DQ2_semantic_accuracy import get_ontology_data, dq2_get_parameters_query

# Con maphier_id_parent_grandparent se obtienen la tabla y el nombre del campo del grandpa
# REVISAR OS: hay una clara diferencia de estilo entre esto y lo que hicimos en el backend
#			  e.g parametros tipeados, funciones que declaran el tipo de lo que retornan, etc.
def get_grandpa_dwatt_colum_str_query1(dq_improvement_method_id, dq_schema):
	return text(f"""
		SELECT dwdim_table, dwatt_column, t7.dataprop_url
		FROM {dq_schema}.dw_dimension						t1
			INNER JOIN {dq_schema}.dw_hierarchy				t2 ON t1.dwdim_id = t2.dwdim_id
			INNER JOIN {dq_schema}.dw_category_position	t3 ON t1.dwdim_id = t3.dwdim_id AND t2.dwhier_id = t3.dwhier_id
			INNER JOIN {dq_schema}.dw_category				t4 ON t3.dwcat_id = t4.dwcat_id
			INNER JOIN {dq_schema}.dw_attribute				t5 ON t3.dwcat_id = t5.dwcat_id
			INNER JOIN {dq_schema}.map_att					t6 ON t3.dwcat_id = t6.dwcat_id AND t5.dwatt_id = t6.dwatt_id
			INNER JOIN {dq_schema}.onto_data_property		t7 ON t6.dataprop_id = t7.dataprop_id
			INNER JOIN {dq_schema}.onto_class 				t8 ON t7.dataprop_domain_id = t8.class_id
			INNER JOIN {dq_schema}.onto_data_type 			t9 ON t7.dataprop_range_id = t9.datatype_id
			INNER JOIN {dq_schema}.map_hier 			   t10 ON t3.dwcat_id = t10.dwcat_parent_id
			INNER JOIN {dq_schema}.dq_improvement		   t11 ON t10.map_id = t11.maphier_id_parent_grandparent
		WHERE t11.dqimprov_method_id = {dq_improvement_method_id}
		""")

def get_grandpa_dwatt_colum_str_query2(dq_improvement_method_id, dq_schema):
	return text(f"""
		SELECT dwdim_table, dwatt_column, t7.dataprop_url
		FROM {dq_schema}.dw_dimension						t1
			INNER JOIN {dq_schema}.dw_hierarchy				t2 ON t1.dwdim_id = t2.dwdim_id
			INNER JOIN {dq_schema}.dw_category_position	t3 ON t1.dwdim_id = t3.dwdim_id AND t2.dwhier_id = t3.dwhier_id
			INNER JOIN {dq_schema}.dw_category				t4 ON t3.dwcat_id = t4.dwcat_id
			INNER JOIN {dq_schema}.dw_attribute				t5 ON t3.dwcat_id = t5.dwcat_id
			INNER JOIN {dq_schema}.map_att					t6 ON t3.dwcat_id = t6.dwcat_id AND t5.dwatt_id = t6.dwatt_id
			INNER JOIN {dq_schema}.onto_data_property		t7 ON t6.dataprop_id = t7.dataprop_id
			INNER JOIN {dq_schema}.onto_class 				t8 ON t7.dataprop_domain_id = t8.class_id
			INNER JOIN {dq_schema}.onto_data_type 			t9 ON t7.dataprop_range_id = t9.datatype_id
			INNER JOIN {dq_schema}.dq_improvement		   t10 ON t6.map_id = t10.map_id_grandparent
		WHERE t10.dqimprov_method_id = {dq_improvement_method_id}
		""")


def get_grandparent_info(dq_improvement_method_id):
	# --- Database connection setup ---
	dq_session = get_DBsession()
	dq_schema = get_dq_schema()
	get_grandpa_dwatt_colum_query = get_grandpa_dwatt_colum_str_query2(dq_improvement_method_id, dq_schema)
	log_verbose(get_grandpa_dwatt_colum_query)
	try:
		result = dq_session.execute(get_grandpa_dwatt_colum_query)
		grandpa_info_list = [dict(row._mapping) for row in result]
		log_result(f"Grandpa info retrieved for IMP1 Completeness Improvement.\n {grandpa_info_list}")
		dq_session.close()
		#REVISAR OS: en las otras reglas este if quedo despues de la exception y eso esta mal.
		if len(grandpa_info_list) != 1:
			log_error("Error one row was expected.")
			raise
	except Exception as e:
		log_error(f"Error retrieving getting grand parent information:  {e}")
		dq_session.close()
		raise
	grandpa_info = dict_to_namedtuple("DQParams", grandpa_info_list[0])
	return grandpa_info

def get_param_imp1_str_query(dq_improvement_method_id, dq_schema):
	return text(f"""
		SELECT 
			t1.dqappliedmethod_id,
			t5.objectprop_url 		AS child_objectprop_url_parent,
			t6.class_url 			AS child_class_url,
			t7.class_url			AS parent_class_url,
			t8.objectprop_url		AS parent_objectprop_url_grandparent,
			t9.class_url 			AS parent_class_url_2,
			t10.class_url			AS grandparent_class_url
		FROM {dq_schema}.dq_improvement 						t1
		INNER JOIN {dq_schema}.dq_appmethod_map_assoc			t2 ON t2.appmethod_id = t1.dqappliedmethod_id
		INNER JOIN {dq_schema}.map_hier 						t3 ON t3.map_id = t2.map_id
		INNER JOIN {dq_schema}.map_hier 						t4 ON t4.map_id = t1.maphier_id_parent_grandparent
		INNER JOIN {dq_schema}.onto_object_property 			t5 ON t5.objectprop_id = t3.objprop_id
		INNER JOIN {dq_schema}.onto_class         				t6 ON t6.class_id = t5.domain_id
		INNER JOIN {dq_schema}.onto_class         				t7 ON t7.class_id = t5.range_id
		INNER JOIN {dq_schema}.onto_object_property 			t8 ON t8.objectprop_id = t4.objprop_id
		INNER JOIN {dq_schema}.onto_class         				t9 ON t9.class_id = t8.domain_id
		INNER JOIN {dq_schema}.onto_class         				t10 ON t10.class_id = t8.range_id
		WHERE t1.dqimprov_method_id = {dq_improvement_method_id}
		"""
	)

def	get_param_dq_imp1(dq_improvement_method_id):
	dq_session = get_DBsession()
	dq_schema = get_dq_schema()

	imp1_get_parameter_query = get_param_imp1_str_query(dq_improvement_method_id, dq_schema)
	log_verbose(imp1_get_parameter_query)
	try:
		result = dq_session.execute(imp1_get_parameter_query)
		dq_imp1_parameters_list = [dict(row._mapping) for row in result]
		log_result(f"DQ_IMP1 parameters \n {dq_imp1_parameters_list}")
		dq_session.close()
		if len(dq_imp1_parameters_list) != 1:
			log_error("Error one row was expected.")
			raise
		#Imp1_params tuple: ( dqappliedmethod_id, 
		#					  child_objectprop_url_parent, child_url, parent_url, 
		#					  parent_objectprop_url_grandparent, parent_url_2, grandparent_url)
		t1 = dict_to_namedtuple("DQParams", dq_imp1_parameters_list[0])
		#dq2_params tuple: ( dwdim_name, dwdim_table, objectprop_url,
		#                       parent_dwatt_column, parent_class_url,parent_dataprop_url, parent_datatype_name,
		#                       child_dwatt_column, child_class_url, child_dataprop_url, child_datatype_name )
		t2 = dq2_get_parameters_query(t1.dqappliedmethod_id)
	except Exception as e:
		log_error(f"Error retrieving getting DQ_IMP1 parameters:  {e}")
		dq_session.close()
		raise
	return t1, t2

def get_ontology_filteredsubgenres(child, grandparent, grandpa_dataprop_url, grandpa_parent_object_prop_url, child_parent_info):
	# --- Get the ontology data using SPARQL ---
	onto = load_ontology()
	#child_parent_info tuple: 
	#(
	# dwdim_name, dwdim_table, objectprop_url,
	# parent_dwatt_column, parent_class_url,parent_dataprop_url, parent_datatype_name,
	# child_dwatt_column, child_class_url, child_dataprop_url, child_datatype_name )
	child_class_url						= child_parent_info.child_class_url
	child_dataprop_url					= child_parent_info.child_dataprop_url
	parent_dataprop_url					= child_parent_info.parent_dataprop_url
	objectprop_urlChildToParent			= child_parent_info.objectprop_url
	grandparent_dataprop_url			= grandpa_dataprop_url
	objectprop_urlParentToGrandParent	= grandpa_parent_object_prop_url
	
	#REVISAR OS: Esta object property no la tenemos. Y es lo que mencionaba que pusimos en la logica, pero no existe en tu teoria.
	objectprop_urlChildToGrandParent	= "http://www.semanticweb.org/csanz/ontologies/2021/10/untitled-ontology-86#hasGenre"
	#child_class_url = "http://www.semanticweb.org/csanz/ontologies/2021/10/untitled-ontology-86#Book"
	#child_dataprop_url = "http://www.semanticweb.org/csanz/ontologies/2021/10/untitled-ontology-86#book_has_name"
	#parent_dataprop_url = "http://www.semanticweb.org/csanz/ontologies/2021/10/untitled-ontology-86#subgenre_has_name"
	# objectprop_urlChildToParent = "http://www.semanticweb.org/csanz/ontologies/2021/10/untitled-ontology-86#hasSubgenre"
	# objectprop_urlChildToGrandParent = "http://www.semanticweb.org/csanz/ontologies/2021/10/untitled-ontology-86#hasGenre"
	# grandparent_dataprop_url = "http://www.semanticweb.org/csanz/ontologies/2021/10/untitled-ontology-86#genre_has_name"
	# objectprop_urlParentToGrandParent =  "http://www.semanticweb.org/csanz/ontologies/2021/10/untitled-ontology-86#genreOfSubgenre"
	
     # --- Query the ontology ---
	sparql_query1 = f"""
	SELECT
        ?domain_dp 
		?range_dp 
		?grandPa_dp
    WHERE {{
        ?domain_class a <{child_class_url}> .
        ?domain_class <{child_dataprop_url}> ?domain_dp .
		FILTER(?domain_dp = "{child}")
		?domain_class <{objectprop_urlChildToParent}> ?range_class .
        ?range_class <{parent_dataprop_url}> ?range_dp .
		?domain_class <{objectprop_urlChildToGrandParent}> ?grandPa_class .
		?range_class <{objectprop_urlParentToGrandParent}> ?grandPa_class .
        ?grandPa_class <{grandparent_dataprop_url}> ?grandPa_dp .
		FILTER(?grandPa_dp = "{grandparent}")

    }}
    """
	sparql_query2 = f"""
	SELECT
        ?domain_dp 
		?range_dp 
		?grandPa_dp
    WHERE {{
        ?domain_class a <{child_class_url}> .
        ?domain_class <{child_dataprop_url}> ?domain_dp .
		FILTER(?domain_dp = "{child}")
		?domain_class <{objectprop_urlChildToParent}> ?range_class .
		?range_class <{parent_dataprop_url}> ?range_dp .
		?range_class <{objectprop_urlParentToGrandParent}> ?grandPa_class .
        ?grandPa_class <{grandparent_dataprop_url}> ?grandPa_dp .
		FILTER(?grandPa_dp = "{grandparent}")
    }}
    """
    # Execute the query
	with onto:
		onto_query = list(default_world.sparql(sparql_query2))
	log_result(f"\n---------------\nOntology Query Result\n---------------\n{onto_query}")
	return onto_query	

def imp1_completeness(dq_improv_id): 
	log_milestone(f"Starting IMP1 Improvement Completeness Rule for - {dq_improv_id}")
	grandparent_info = get_grandparent_info(dq_improv_id)
	t1, t2 = get_param_dq_imp1(dq_improv_id)
	improve_candidates = dq_completeness(t1.dqappliedmethod_id, grandparent_info.dwatt_column)

	result = []
	for tpl in improve_candidates:
		grandparent = tpl[0]
		child = tpl[1]
		candidates = [s.strip() for s in tpl[-1].split('/')]
		filterd_improv = get_ontology_filteredsubgenres(child, grandparent,
											grandparent_info.dataprop_url, t1.parent_objectprop_url_grandparent, 
											t2)
		result.append(filterd_improv)									
		log_result(f"Improvement Completeness :\n{result}")
	return result
		


    
