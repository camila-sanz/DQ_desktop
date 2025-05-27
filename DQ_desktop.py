0
# -*- coding: utf-8 -*-

from modules.db_handler import *
from modules.dq_rules.DQ1_syntactic_accuracy import dq_syntactic_accuracy
from modules.dq_rules.DQ2_semantic_accuracy import dq_semantic_accuracy
from modules.dq_rules.DQ3_completeness import dq_completeness
from modules.dq_rules.DQR4_consistency import dq_consistency
from modules.dq_rules.IMP_completeness import imp1_completeness
from modules.logger.shared_logger import *


def main():
    setup_logger(LOG_VERBOSE, "desktopdq.log")  

    log_milestone("Application started.")
    while True:
        # Configurar la conexión a PostgreSQL
        dq_session = get_DBsession()
        dq_schema = get_dq_schema()

        # Consulta para obtener los métodos disponibles
        query = text(f"""
                    SELECT 
                            apmethod_id                 AS method_id,
                            apmethod_name               AS method_name, 
                            apmethod_description        AS method_description,
                            'DQ' || dqmethod_id         AS dq_algorithm_id
                    FROM {dq_schema}.dq_appmethod
                UNION
                    SELECT 
                            dqimprov_method_id          AS method_id,
                            dqimprovement_name          AS method_name, 
                            dqimprov_description        AS method_description,
                            'IMP' || dqimprovement_id   AS dq_algorithm_id
                    FROM {dq_schema}.dq_improvement
                ORDER BY method_id
                    """
                 )
        methods = dq_session.execute(query).fetchall()
        dq_session.close()

        # Verifica
        # r si hay métodos en la tabla
        if not methods:
            print("No se encontraron métodos en la tabla dq_app_method_name.")
            exit()

        # Mostrar los métodos disponibles
        print("\nMétodos disponibles:")
                         
        for idx, (method_id, method_name, method_description, dq_algorithm_id) in enumerate(methods, start=1):
            print(f"{idx:2} - {method_name:50} ID: {method_id:2}, Método: {dq_algorithm_id:4}-{method_description}")
            

        # Pedir al usuario que seleccione métodos
        selected_indices = input("\nSeleccione los métodos (números separados por coma): ").split(",")

        # Filtrar los métodos seleccionados
        selected_methods = []
        for idx in selected_indices:
            try:
                selected_methods.append(methods[int(idx) - 1])  # Convertir índice a entero
            except (ValueError, IndexError):
                print(f"Índice inválido: {idx}")

        # Verificar si hay métodos seleccionados
        if not selected_methods:
            print("No se seleccionaron métodos válidos.")
            exit()

        # Mostrar los métodos seleccionados
        print("\nMétodos seleccionados:")
        for (method_id, method_name, method_description, dq_algorithm_id) in selected_methods:
            print(f"{method_id} - {method_name} : {dq_algorithm_id}- {method_description}")

        # Función para ejecutar el módulo correspondiente
        def ejecutar_metodo(dq_algorithm_id, method_id):
            if dq_algorithm_id == 'DQ1':
                dq_syntactic_accuracy(method_id)
            elif dq_algorithm_id == 'DQ2':
                dq_semantic_accuracy(method_id)
            elif dq_algorithm_id == 'DQ3':
                dq_completeness(method_id)
            elif dq_algorithm_id == 'DQ4':
                dq_consistency(method_id)
            elif dq_algorithm_id == 'IMP1':
                imp1_completeness(method_id)
            else:
                print(f"Método '{dq_algorithm_id}' no reconocido.")

        # Recorrer la lista de métodos seleccionados y ejecutarlos
        for method_id, method_name, method_description, dq_algorithm_id in selected_methods:
            print(f"\nEjecutando: {method_id}) {method_description} para {method_name} [{dq_algorithm_id}]")
            ejecutar_metodo(dq_algorithm_id, method_id)


if __name__ == "__main__":
    main()


