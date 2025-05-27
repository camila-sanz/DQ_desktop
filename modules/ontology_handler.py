from owlready2 import *

ONTOLOGY_PATH = r"C:\Users\csanz\Documents\FING\Doctorado\Tesis\QDB2025\for onto\moviekg_full_errors.owl"

def load_ontology():
    """ Carga y devuelve la ontología """
    onto = get_ontology(ONTOLOGY_PATH).load() #REVISAR sacar de la base de datos
    return onto

def execute_sparql_query(query):
    """ Ejecuta una consulta SPARQL en la ontología """
    return default_world.sparql(query)

