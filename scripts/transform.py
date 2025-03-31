import sqlite3
import os

# Pistas 
# 1. Conectarse a la base de datos donde están las tablas Silver
# 2. Guarda los queries realizados en el trabajo pasado como un string

def transform_data():
    db_path = '/opt/airflow/dags/data/ecommerce.db'
    #TODO conexión

    #TODO Query 1: Top 10 estados con mayor ingreso (str)
    
    #TODO Query 2: Comparación de tiempos reales vs estimados por mes y año (str)
    


    print("🚀 Ejecutando queries para crear tablas Gold...")
    cursor.executescript(query1)
    cursor.executescript(query2)

    conn.commit()
    conn.close()
    print("✅ Tablas Gold creadas en ecommerce.db: 'gold_top_states' y 'gold_delivery_comparison'")
