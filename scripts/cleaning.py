import sqlite3
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin
#Pistas
# En este paso la idea sería quitar duplicados, manejar nulos, pero como no es el objetivo, 
# Vamos a hacer una copia espejo de los datos, simulando que los datos ya están limpios.
# 1.Conectarse a la base de datos ecommerce.db ubicada en /opt/airflow/dags/data
# 2: Elimine la tabla Silver si ya existe, cree una tabla nueva Silver copiando 
#    todo el contenido de su tabla Bronze correspondiente
#    Cada bloque debe hacer una copia de la tabla Bronze a una nueva tabla Silver
# 3: Guardar los cambios y cerrar la conexión
# 4: Usa print() para mostrar el estado del proceso

log = LoggingMixin().log

def cleaning_data():
    db_path = '/opt/airflow/dags/data/ecommerce.db'

    conn = sqlite3.connect(db_path)

    # Diccionario: tabla bronze → tabla silver
    tables = {
        'bronze_olist_orders': 'silver_olist_orders',
        'bronze_olist_order_payments': 'silver_olist_order_payments',
        'bronze_olist_customers': 'silver_olist_customers'
    }

    for bronze_table, silver_table in tables.items():
        try:
            df = pd.read_sql_query(f"SELECT * FROM {bronze_table}", conn)

            log.info(f"Datos cargados desde {bronze_table}: {df.shape[0]} filas.")

            df.to_sql(silver_table, conn, if_exists='replace', index=False)
            log.info(f"Tabla '{silver_table}' creada a partir de '{bronze_table}'.")
        
        except Exception as e:
            log.error(f"Error al copiar {bronze_table} a {silver_table}: {e}")

    conn.close()
    log.info("Todas las tablas han sido copiadas de bronze a silver.")
