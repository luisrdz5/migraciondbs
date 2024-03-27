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

def get_table_indexes(cursor, db_type, table_name):
    index_query = {
        "mysql": f"SHOW INDEX FROM {table_name}",
        "sqlserver": f"SELECT index_name FROM sys.indexes WHERE object_id = OBJECT_ID('{table_name}')",
        "postgresql": f"SELECT indexname FROM pg_indexes WHERE tablename = '{table_name}'",
    }[db_type]
    cursor.execute(index_query)
    return [row[0] for row in cursor.fetchall()]

def compare_table_counts(cursor1, cursor2, table_name):
    query = f"SELECT COUNT(*) FROM {table_name}"
    cursor1.execute(query)
    count1 = cursor1.fetchone()[0]
    cursor2.execute(query)
    count2 = cursor2.fetchone()[0]
    return (count1, count2)

def compare_table_details(cursor1, cursor2, db_type1, db_type2, table_name):
    count1, count2 = compare_table_counts(cursor1, cursor2, table_name)
    indexes1 = get_table_indexes(cursor1, db_type1, table_name)
    indexes2 = get_table_indexes(cursor2, db_type2, table_name)
    index_differences = set(indexes1) ^ set(indexes2)
    return (count1, count2, indexes1, indexes2, index_differences)

# Nombre del archivo de log
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
table.field_names = ["Tabla", "Servidor 1 (Registros)", "Servidor 2 (Registros)", "Estado Registros", "Índices Servidor 1", "Índices Servidor 2", "Diferencias Índices"]

with get_db_connection(os.getenv('DB_TYPE_1'), os.getenv('HOST_1'), os.getenv('DB_NAME_1'), os.getenv('USER_1'), os.getenv('PASSWORD_1')) as conn1, \
     get_db_connection(os.getenv('DB_TYPE_2'), os.getenv('HOST_2'), os.getenv('DB_NAME_2'), os.getenv('USER_2'), os.getenv('PASSWORD_2')) as conn2:
    
    cursor1 = conn1.cursor()
    cursor2 = conn2.cursor()
    tables1 = get_tables(cursor1, os.getenv('DB_TYPE_1'))
    tables2 = get_tables(cursor2, os.getenv('DB_TYPE_2'))

    common_tables = set(tables1) & set(tables2)
    for table_name in common_tables:
        count1, count2, indexes1, indexes2, index_differences = compare_table_details(cursor1, cursor2, os.getenv('DB_TYPE_1'), os.getenv('DB_TYPE_2'), table_name)
        differences = abs(count1 - count2)
        status_records = "Iguales" if count1 == count2 else f"Diferentes: diferencia de {differences} registros"
        status_indexes = "Iguales" if not index_differences else f"Diferentes: {len(index_differences)} índices distintos"
        
        # Modificado para mostrar en pantalla el número de índices en vez de sus nombres
        table.add_row([table_name, f"{count1} registros", f"{count2} registros", status_records, f"{len(indexes1)} índices", f"{len(indexes2)} índices", status_indexes])

    # Escribir resultados en consola
    print(table)

    # Preparar la misma tabla pero con el detalle de índices para el archivo de log
    detailed_table = PrettyTable()
    detailed_table.field_names = ["Tabla", "Servidor 1 (Registros)", "Servidor 2 (Registros)", "Estado Registros", "Índices Servidor 1", "Índices Servidor 2", "Diferencias Índices"]
    for table_name in common_tables:
        count1, count2, indexes1, indexes2, index_differences = compare_table_details(cursor1, cursor2, os.getenv('DB_TYPE_1'), os.getenv('DB_TYPE_2'), table_name)
        differences = abs(count1 - count2)
        status_records = "Iguales" if count1 == count2 else f"Diferentes: diferencia de {differences} registros"
        status_indexes = "Iguales" if not index_differences else f"Diferentes: {', '.join(index_differences)}"
        detailed_table.add_row([table_name, f"{count1} registros", f"{count2} registros", status_records, ', '.join(indexes1), ', '.join(indexes2), status_indexes])

    # Escribir el detalle en el archivo de log
    with open(log_file_name, "a") as log_file:
        log_file.write("\n" + "-" * 64 + "\n")
        log_file.write(f"----  Datos Resumidos  ----\n")
        log_file.write("-" * 64 + "\n")
        log_file.write(str(table))
        log_file.write("\n") 
        log_file.write("\n" + "-" * 64 + "\n")
        log_file.write(f"----  Detalle de Indices  ----\n")
        log_file.write("-" * 64 + "\n")
        log_file.write(str(detailed_table))
        log_file.write("\n") 