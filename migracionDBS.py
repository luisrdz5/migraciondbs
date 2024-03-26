import os
import datetime
from sqlalchemy import create_engine

# Configuración de variables de entorno
DB_TYPE_1 = os.environ['DB_TYPE_1']  # Tipo de DB: mysql, postgresql, mssql
DB_HOST_1 = os.environ['DB_HOST_1']
DB_NAME_1 = os.environ['DB_NAME_1']
DB_USER_1 = os.environ['DB_USER_1']
DB_PASS_1 = os.environ['DB_PASS_1']

DB_TYPE_2 = os.environ['DB_TYPE_2']  # Tipo de DB: mysql, postgresql, mssql
DB_HOST_2 = os.environ['DB_HOST_2']
DB_NAME_2 = os.environ['DB_NAME_2']
DB_USER_2 = os.environ['DB_USER_2']
DB_PASS_2 = os.environ['DB_PASS_2']

def create_engine_url(db_type, user, password, host, db_name):
    if db_type == 'mysql':
        return f'mysql+pymysql://{user}:{password}@{host}/{db_name}'
    elif db_type == 'postgresql':
        return f'postgresql://{user}:{password}@{host}/{db_name}'
    elif db_type == 'mssql':
        return f'mssql+pyodbc://{user}:{password}@{host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server'
    else:
        raise ValueError("Tipo de base de datos no soportado.")

# Crear motores de conexión para ambas bases de datos
engine_1 = create_engine(create_engine_url(DB_TYPE_1, DB_USER_1, DB_PASS_1, DB_HOST_1, DB_NAME_1))
engine_2 = create_engine(create_engine_url(DB_TYPE_2, DB_USER_2, DB_PASS_2, DB_HOST_2, DB_NAME_2))

def compare_table_counts(engine1, engine2):
    with engine1.connect() as conn1, engine2.connect() as conn2:
        res1 = conn1.execute("SHOW TABLES")
        tables1 = [row[0] for row in res1]
        
        for table in tables1:
            count1 = conn1.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            try:
                count2 = conn2.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                difference = abs(count1 - count2)
                print(f"Tabla: {table} | Count1: {count1} | Count2: {count2} | Diferencia: {difference}")
                # Aquí agregarías el código para escribir en el archivo
            except Exception as e:
                print(f"Error con la tabla {table}: {e}")

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    print(f"Inicio de la ejecución: {start_time}")
    compare_table_counts(engine_1, engine_2)
    end_time = datetime.datetime.now()
    print(f"Fin de la ejecución: {end_time}")
    duration = end_time - start_time
    print(f"Duración de la ejecución: {duration}")
