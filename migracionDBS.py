import os
from dotenv import load_dotenv
import pymysql
import pyodbc
import psycopg2
from prettytable import PrettyTable
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

# Conexión a las bases de datos
connections = {
    "mysql": lambda db_info: pymysql.connect(host=db_info['host'], user=db_info['user'], password=db_info['password'], database=db_info['db_name']),
    "sqlserver": lambda db_info: pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+db_info['host']+';DATABASE='+db_info['db_name']+';UID='+db_info['user']+';PWD='+ db_info['password']),
    "postgresql": lambda db_info: psycopg2.connect(host=db_info['host'], dbname=db_info['db_name'], user=db_info['user'], password=db_info['password']),
}

def get_db_connection(db_type, host, db_name, user, password):
    return connections[db_type]({
        'host': host,
        'db_name': db_name,
        'user': user,
        'password': password,
    })

def get_tables(cursor, db_type):
    query = {
        "mysql": "SHOW TABLES",
        "sqlserver": "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'",
        "postgresql": "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    }[db_type]
    cursor.execute(query)
    if db_type == "mysql":
        return [row[0] for row in cursor]
    else:
        return [row[0] for row in cursor.fetchall()]

def compare_table_counts(cursor1, cursor2, table_name):
    query = f"SELECT COUNT(*) FROM {table_name}"
    cursor1.execute(query)
    count1 = cursor1.fetchone()[0]
    cursor2.execute(query)
    count2 = cursor2.fetchone()[0]
    return (count1, count2)

# Nombre del archivo de log, constante para mantener historial
log_file_name = "db_comparisons_history.txt"

# Función para agregar el encabezado de la ejecución al archivo de log
def add_execution_header_to_log():
    with open(log_file_name, "a") as log_file:
        log_file.write("\n" + "-" * 64 + "\n")
        log_file.write(f"----  Ejecución: {datetime.now().strftime('%d/%m/%Y %I:%M %p')}  ----\n")
        log_file.write("-" * 64 + "\n")

# Iniciar script
add_execution_header_to_log()

# Crear la tabla para el registro y la visualización
table = PrettyTable()
table.field_names = ["Tabla", "Servidor 1", "Servidor 2", "Estado"]

with get_db_connection(os.getenv('DB_TYPE_1'), os.getenv('HOST_1'), os.getenv('DB_NAME_1'), os.getenv('USER_1'), os.getenv('PASSWORD_1')) as conn1, \
     get_db_connection(os.getenv('DB_TYPE_2'), os.getenv('HOST_2'), os.getenv('DB_NAME_2'), os.getenv('USER_2'), os.getenv('PASSWORD_2')) as conn2:
    
    cursor1 = conn1.cursor()
    cursor2 = conn2.cursor()
    tables1 = get_tables(cursor1, os.getenv('DB_TYPE_1'))
    tables2 = get_tables(cursor2, os.getenv('DB_TYPE_2'))

    common_tables = set(tables1) & set(tables2)
    for table_name in common_tables:
        count1, count2 = compare_table_counts(cursor1, cursor2, table_name)
        differences = abs(count1 - count2)
        status = "Iguales" if count1 == count2 else f"Diferentes: diferencia de {differences} registros"
        table.add_row([table_name, f"{count1} registros", f"{count2} registros", status])

    # Si deseas comparar tablas únicas, agrega esa lógica aquí

# Escribir resultados en consola y archivo de log
# Escribir resultados en consola y archivo de log
print(table)
with open(log_file_name, "a") as log_file:
    log_file.write(str(table))
    log_file.write("\n")  # Asegurar una nueva línea al final para separación entre ejecuciones

