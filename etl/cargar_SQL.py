import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURACIÓN DE CONEXIÓN ---
# CAMBIA ESTO CON TUS DATOS REALES
USER = 'root'
PASSWORD = 'tu_contraseña' 
HOST = 'localhost'
PORT = '3306'
DB_NAME = 'SG_Empresa'

# Cadena de conexión (usando pymysql)
connection_string = f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}"
engine = create_engine(connection_string)

def cargar_tabla(nombre_csv, nombre_tabla, engine):
    try:
        print(f"Cargando {nombre_tabla} desde {nombre_csv}...")
        df = pd.read_csv(nombre_csv)
        
        # 'append' agrega datos. Como truncamos antes, es seguro.
        # index=False evita que se inserte el índice de pandas como columna
        df.to_sql(nombre_tabla, con=engine, if_exists='append', index=False)
        print(f"OK: {len(df)} registros insertados en {nombre_tabla}.")
    except Exception as e:
        print(f"ERROR cargando {nombre_tabla}: {e}")

# --- EJECUCIÓN ---
with engine.connect() as conn:
    # 1. Limpiar base de datos (Opcional: borra datos previos para no duplicar)
    print("Limpiando tablas existentes...")
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
    tablas = ['Finanzas', 'Prueba', 'Tarea', 'AsignacionProyecto', 'Empleado', 'Proyecto', 'Metodologia', 'Cliente']
    for t in tablas:
        conn.execute(text(f"TRUNCATE TABLE {t};"))
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
    print("Tablas limpias.")

    # 2. Cargar CSVs en ORDEN de Dependencias (Padre -> Hijo)
    cargar_tabla('clientes.csv', 'Cliente', engine)
    cargar_tabla('metodologias.csv', 'Metodologia', engine)
    cargar_tabla('empleados.csv', 'Empleado', engine)
    
    # Proyecto depende de Cliente y Metodologia
    cargar_tabla('proyectos.csv', 'Proyecto', engine)
    
    # Asignacion depende de Empleado y Proyecto
    cargar_tabla('asignaciones.csv', 'AsignacionProyecto', engine)
    
    # Tarea y Prueba dependen de Asignacion
    cargar_tabla('tareas.csv', 'Tarea', engine)
    cargar_tabla('pruebas.csv', 'Prueba', engine)
    
    # Finanzas depende de Proyecto
    cargar_tabla('finanzas.csv', 'Finanzas', engine)

print("\n--- ¡Carga de datos completada exitosamente! ---")