import sqlite3

#Pistas
# En este paso la idea sería quitar duplicados, manejar nulos, pero como no es el objetivo, 
# Vamos a hacer una copia espejo de los datos, simulando que los datos ya están limpios.
# 1.Conectarse a la base de datos ecommerce.db ubicada en /opt/airflow/dags/data
# 2: Elimine la tabla Silver si ya existe, cree una tabla nueva Silver copiando 
#    todo el contenido de su tabla Bronze correspondiente
#    Cada bloque debe hacer una copia de la tabla Bronze a una nueva tabla Silver
# 3: Guardar los cambios y cerrar la conexión
# 4: Usa print() para mostrar el estado del proceso

def cleaning_data():
    db_path = '/opt/airflow/dags/data/ecommerce.db'
    #TODO
