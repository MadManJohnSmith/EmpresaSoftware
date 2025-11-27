import pandas as pd
import os
import json
import numpy as np
from datetime import datetime

# --- CONFIGURACIÓN DE DIRECTORIOS ---
SOURCE_DIR = 'csv'                
DW_DIR = 'data_warehouse_csv'           # Carpeta para el DW (IDs)
OLAP_DIR = 'cubo_olap_csv'              # Carpeta para el Cubo legible (Nombres)
STATE_FILE = 'etl_state_tracking.json'  # Control de estado

# Crear carpetas si no existen
os.makedirs(DW_DIR, exist_ok=True)
os.makedirs(OLAP_DIR, exist_ok=True)

# ==============================================================================
# GESTOR DE ARCHIVOS Y DIMENSIONES
# ==============================================================================
class DataWarehouseManager:
    def __init__(self, dw_directory):
        self.dw_dir = dw_directory

    def get_or_create_table(self, table_name, columns):
        """
        Lógica central solicitada:
        1. Comprueba si existe el archivo CSV.
        2. SI EXISTE -> Lo lee.
        3. NO EXISTE -> Lo crea vacío con encabezados y lo guarda físicamente.
        """
        file_path = os.path.join(self.dw_dir, f"{table_name}.csv")
        
        if os.path.exists(file_path):
            # print(f"   [IO] Leyendo {table_name}.csv existente...")
            return pd.read_csv(file_path)
        else:
            print(f"   [IO] Creando nuevo archivo: {table_name}.csv")
            df_empty = pd.DataFrame(columns=columns)
            df_empty.to_csv(file_path, index=False)
            return df_empty

    def save_table(self, df, table_name):
        """Sobreescribe la tabla completa (útil para dimensiones pequeñas que se regeneran)."""
        file_path = os.path.join(self.dw_dir, f"{table_name}.csv")
        df.to_csv(file_path, index=False)

    def append_to_table(self, df_new, table_name):
        """Agrega filas al final del archivo CSV sin reescribir todo."""
        if df_new.empty: return
        file_path = os.path.join(self.dw_dir, f"{table_name}.csv")
        # mode='a' es append, header=False porque el archivo ya debe existir (creado por get_or_create)
        df_new.to_csv(file_path, mode='a', header=False, index=False)

    def sync_catalog_dimension(self, new_values_series, table_name, id_col, val_col):
        """
        Sincroniza catálogos simples (País, Industria).
        Revisa si el valor existe, si no, le asigna ID y lo guarda.
        """
        # 1. Obtener o Crear
        df_dim = self.get_or_create_table(table_name, columns=[id_col, val_col])
        
        # 2. Mapa actual en memoria
        current_map = dict(zip(df_dim[val_col], df_dim[id_col]))
        
        # 3. Detectar nuevos
        incoming = new_values_series.dropna().unique()
        new_records = []
        next_id = 1 if df_dim.empty else df_dim[id_col].max() + 1
        
        for val in incoming:
            if val not in current_map:
                current_map[val] = next_id
                new_records.append({id_col: next_id, val_col: val})
                next_id += 1
        
        # 4. Guardar cambios si hubo
        if new_records:
            df_new = pd.DataFrame(new_records)
            self.append_to_table(df_new, table_name)
            print(f"   [DIM] {table_name}: +{len(new_records)} registros nuevos.")
            
        return current_map

# ==============================================================================
# CONTROLADOR DE ESTADO
# ==============================================================================
class StateManager:
    def __init__(self):
        self.state = self._load()
    
    def _load(self):
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
                # Normalizar claves para compatibilidad
                if 'last_id_proyecto_scanned' in state:
                    state['last_id_scanned'] = state.pop('last_id_proyecto_scanned')
                if 'pending_project_ids' in state:
                    state['pending_ids'] = state.pop('pending_project_ids')
                if 'last_id_hecho_proy' in state:
                    state['last_id_h_proy'] = state.pop('last_id_hecho_proy')
                if 'last_id_hecho_calidad' in state:
                    state['last_id_h_cal'] = state.pop('last_id_hecho_calidad')
                return state
        # Valores iniciales
        return {"last_id_scanned": 0, "pending_ids": [], "last_id_h_proy": 0, "last_id_h_cal": 0}

    def save(self):
        with open(STATE_FILE, 'w') as f: json.dump(self.state, f, indent=4)

# ==============================================================================
# PROCESO ETL PRINCIPAL
# ==============================================================================
def run_etl():
    print("--- INICIANDO ETL: CHECK EXISTENCE & UPDATE ---")
    dw_mgr = DataWarehouseManager(DW_DIR)
    state_mgr = StateManager()
    
    # 1. CARGA DE FUENTES (Simulación)
    try:
        src_proy = pd.read_csv(os.path.join(SOURCE_DIR, 'proyectos.csv'))
        src_cli = pd.read_csv(os.path.join(SOURCE_DIR, 'clientes.csv'))
        src_asig = pd.read_csv(os.path.join(SOURCE_DIR, 'asignaciones.csv'))
        src_pruebas = pd.read_csv(os.path.join(SOURCE_DIR, 'pruebas.csv'))
    except FileNotFoundError as e:
        print(f"Error: No se encuentran los archivos fuente. Path intentado: {os.path.abspath(os.path.join(SOURCE_DIR, 'proyectos.csv'))}")
        print(f"Detalle error: {e}")
        return

    # 2. FILTRADO (Lógica de Pendientes y Nuevos)
    # Identificar qué proyectos procesaremos hoy (Solo finalizados/cancelados)
    ids_to_process = []
    
    # A. Revisar pendientes anteriores
    if state_mgr.state['pending_ids']:
        pend_df = src_proy[src_proy['id_proyecto'].isin(state_mgr.state['pending_ids'])]
        ready_pend = pend_df[pend_df['estado'].isin(['finalizado', 'cancelado'])]['id_proyecto'].tolist()
        ids_to_process.extend(ready_pend)
        # Quitar los listos de la lista de pendientes
        state_mgr.state['pending_ids'] = [x for x in state_mgr.state['pending_ids'] if x not in ready_pend]

    # B. Revisar nuevos registros
    new_df = src_proy[src_proy['id_proyecto'] > state_mgr.state['last_id_scanned']]
    if not new_df.empty:
        ready_new = new_df[new_df['estado'].isin(['finalizado', 'cancelado'])]['id_proyecto'].tolist()
        waiting_new = new_df[~new_df['estado'].isin(['finalizado', 'cancelado'])]['id_proyecto'].tolist()
        
        ids_to_process.extend(ready_new)
        state_mgr.state['pending_ids'].extend(waiting_new)
        state_mgr.state['last_id_scanned'] = int(new_df['id_proyecto'].max())

    if not ids_to_process:
        print("   -> No hay proyectos nuevos cerrados para procesar."); state_mgr.save(); return

    print(f"   -> Procesando lote de {len(ids_to_process)} proyectos.")
    
    # Dataframes del lote actual
    batch_proy = src_proy[src_proy['id_proyecto'].isin(ids_to_process)].copy()
    batch_cli = src_cli[src_cli['id_cliente'].isin(batch_proy['id_cliente'])].copy()

    # ==========================================================================
    # 3. GESTIÓN DE DIMENSIONES (Usando get_or_create)
    # ==========================================================================
    print("3. [DIMENSIONES] Verificando y Actualizando...")

    # --- A. SUBDIMENSIONES ---
    # Sincronizamos catálogos y obtenemos mapas {Nombre: ID}
    map_pais = dw_mgr.sync_catalog_dimension(batch_cli['pais'], 'SubdimPais', 'id_pais', 'nombre_pais')
    map_ind = dw_mgr.sync_catalog_dimension(batch_cli['industria'], 'SubdimIndustria', 'id_industria', 'nombre_industria')
    
    # Satisfacción (si no existe archivo, creamos el default)
    df_satis = dw_mgr.get_or_create_table('SubdimSatisfaccion', ['id_satisfaccion', 'descripcion'])
    if df_satis.empty:
        defaults = pd.DataFrame({'id_satisfaccion': [1,2,3,4,5], 'descripcion': ['Muy Malo','Malo','Regular','Bueno','Excelente']})
        dw_mgr.save_table(defaults, 'SubdimSatisfaccion')
    # Mapa estático
    map_satis = {1:1, 2:2, 3:3, 4:4, 5:5}

    # --- B. DIMENSIÓN CLIENTE ---
    # 1. Cargar dimensión existente (o crear vacía)
    cols_cli = ['id_cliente', 'nombre_cliente', 'id_industria', 'id_pais', 'id_satisfaccion']
    df_dim_cli = dw_mgr.get_or_create_table('DimCliente', cols_cli)
    
    # 2. Detectar clientes nuevos en este lote
    existing_clients = set(df_dim_cli['id_cliente'])
    new_clients = batch_cli[~batch_cli['id_cliente'].isin(existing_clients)].copy()
    
    if not new_clients.empty:
        # 3. Mapear IDs foráneos
        new_clients['id_pais'] = new_clients['pais'].map(map_pais)
        new_clients['id_industria'] = new_clients['industria'].map(map_ind)
        new_clients['id_satisfaccion'] = new_clients['nivel_satisfaccion'].map(map_satis)
        
        # 4. Guardar solo los nuevos (Append)
        dw_mgr.append_to_table(new_clients[cols_cli], 'DimCliente')
        print(f"   [DIM] DimCliente: +{len(new_clients)} registros.")

    # --- C. DIMENSIÓN PROYECTO ---
    cols_proy = ['id_proyecto', 'nombre_proyecto', 'tipo_prioridad', 'fecha_inicio_planificada', 'fecha_fin_planificada']
    df_dim_proy = dw_mgr.get_or_create_table('DimProyecto', cols_proy)
    
    existing_proys = set(df_dim_proy['id_proyecto'])
    new_dim_proy = batch_proy[~batch_proy['id_proyecto'].isin(existing_proys)].copy()
    
    if not new_dim_proy.empty:
        new_dim_proy = new_dim_proy.rename(columns={'prioridad': 'tipo_prioridad', 'fecha_inicio_estimada': 'fecha_inicio_planificada', 'fecha_fin_estimada': 'fecha_fin_planificada'})
        dw_mgr.append_to_table(new_dim_proy[cols_proy], 'DimProyecto')
        print(f"   [DIM] DimProyecto: +{len(new_dim_proy)} registros.")

    # --- D. DIMENSIÓN TIEMPO (Estática) ---
    df_tiempo = dw_mgr.get_or_create_table('DimTiempo', ['id_tiempo', 'fecha', 'año', 'mes'])
    if df_tiempo.empty:
        dates = pd.date_range('2010-01-01', '2030-12-31')
        df_t = pd.DataFrame({'id_tiempo': dates.strftime('%Y%m%d').astype(int), 'fecha': dates, 'año': dates.year, 'mes': dates.month})
        dw_mgr.save_table(df_t, 'DimTiempo')

    # ==========================================================================
    # 4. GENERACIÓN DE HECHOS (CON IDs)
    # ==========================================================================
    print("4. [HECHOS] Calculando e insertando hechos...")

    # A. Hechos Proyecto
    cols_h_proy = ['id_hecho', 'id_proyecto', 'id_cliente', 'id_metodologia', 'id_tiempo_cierre', 'costo_total_real', 'ganancia_neta']
    # Nos aseguramos que el archivo exista para poder hacer append luego
    dw_mgr.get_or_create_table('HechosProyecto', cols_h_proy)
    
    hechos_p = batch_proy.copy()
    # Calculos
    hechos_p['id_tiempo_cierre'] = pd.to_datetime(hechos_p['fecha_fin_real'].fillna(hechos_p['fecha_inicio_real'])).dt.strftime('%Y%m%d').astype(int)
    hechos_p['costo_total_real'] = hechos_p['presupuesto_real']
    hechos_p['ganancia_neta'] = hechos_p['ganancias']
    
    # ID Autoincremental
    start_id = state_mgr.state['last_id_h_proy'] + 1
    hechos_p['id_hecho'] = range(start_id, start_id + len(hechos_p))
    state_mgr.state['last_id_h_proy'] += len(hechos_p)
    
    dw_mgr.append_to_table(hechos_p[cols_h_proy], 'HechosProyecto')

    # B. Hechos Calidad
    cols_h_cal = ['id_hecho_calidad', 'id_proyecto', 'id_tiempo', 'tipo_prueba', 'severidad', 'cantidad_defectos_encontrados']
    dw_mgr.get_or_create_table('HechosCalidad', cols_h_cal)
    
    # Filtrar pruebas del lote
    batch_asig = src_asig[src_asig['id_proyecto'].isin(ids_to_process)]
    batch_pruebas = src_pruebas[src_pruebas['id_asignacion'].isin(batch_asig['id_asignacion'])]
    
    df_h_cal = pd.DataFrame() # Inicializar vacío por seguridad
    
    if not batch_pruebas.empty:
        m = batch_pruebas.merge(batch_asig[['id_asignacion', 'id_proyecto']], on='id_asignacion')
        m['id_tiempo'] = pd.to_datetime(m['fecha_ejecucion']).dt.strftime('%Y%m%d').astype(int)
        m['es_defecto'] = np.where(m['resultado']=='fallo', 1, 0)
        
        df_h_cal = m.groupby(['id_proyecto', 'id_tiempo', 'tipo', 'severidad_defecto']).agg({'es_defecto': 'sum'}).reset_index()
        df_h_cal.columns = ['id_proyecto', 'id_tiempo', 'tipo_prueba', 'severidad', 'cantidad_defectos_encontrados']
        
        start_cal = state_mgr.state['last_id_h_cal'] + 1
        df_h_cal.insert(0, 'id_hecho_calidad', range(start_cal, start_cal + len(df_h_cal)))
        state_mgr.state['last_id_h_cal'] += len(df_h_cal)
        
        dw_mgr.append_to_table(df_h_cal, 'HechosCalidad')

    # ==========================================================================
    # 5. GENERACIÓN DE CUBO OLAP (Flat Files para Analisis)
    # ==========================================================================
    print("5. [OLAP] Generando vistas desnormalizadas...")
    
    # Recargar dimensiones completas para hacer los joins
    full_dim_proy = dw_mgr.get_or_create_table('DimProyecto', cols_proy)
    full_dim_cli = dw_mgr.get_or_create_table('DimCliente', cols_cli)
    full_pais = dw_mgr.get_or_create_table('SubdimPais', ['id_pais', 'nombre_pais'])
    
    # Denormalizar Proyecto (Unir Hechos Recientes + Dimensiones)
    # Nota: Aquí usamos 'hechos_p' (el lote actual) para append al cubo, 
    # o podríamos recargar 'HechosProyecto' completo si quisieras regenerar todo el cubo.
    # Haremos Append del lote actual al archivo OLAP.
    
    flat_proy = hechos_p[cols_h_proy].merge(full_dim_proy[['id_proyecto', 'nombre_proyecto']], on='id_proyecto', how='left')
    flat_proy = flat_proy.merge(full_dim_cli[['id_cliente', 'nombre_cliente']], on='id_cliente', how='left')
    flat_proy = flat_proy.merge(full_pais, left_on='id_cliente', right_on='id_pais', how='left')
    
    # Obtener id_pais desde DimCliente para el join correcto
    flat_proy = hechos_p[cols_h_proy].merge(full_dim_proy[['id_proyecto', 'nombre_proyecto']], on='id_proyecto', how='left')
    flat_proy = flat_proy.merge(full_dim_cli[['id_cliente', 'nombre_cliente', 'id_pais']], on='id_cliente', how='left')
    flat_proy = flat_proy.merge(full_pais, on='id_pais', how='left')
    
    cols_olap_p = ['id_hecho', 'nombre_proyecto', 'nombre_cliente', 'nombre_pais', 'ganancia_neta', 'costo_total_real']
    
    # Guardar en carpeta OLAP (Verifica si existe, si no crea con header, luego append)
    path_olap_p = os.path.join(OLAP_DIR, 'OLAP_Proyectos.csv')
    if not os.path.exists(path_olap_p):
        flat_proy[cols_olap_p].to_csv(path_olap_p, index=False)
    else:
        flat_proy[cols_olap_p].to_csv(path_olap_p, mode='a', header=False, index=False)

    # Denormalizar Calidad
    if not df_h_cal.empty:
        flat_cal = df_h_cal.merge(full_dim_proy[['id_proyecto', 'nombre_proyecto']], on='id_proyecto', how='left')
        cols_olap_c = ['id_hecho_calidad', 'nombre_proyecto', 'id_tiempo', 'tipo_prueba', 'severidad', 'cantidad_defectos_encontrados']
        
        path_olap_c = os.path.join(OLAP_DIR, 'OLAP_Calidad.csv')
        if not os.path.exists(path_olap_c):
            flat_cal[cols_olap_c].to_csv(path_olap_c, index=False)
        else:
            flat_cal[cols_olap_c].to_csv(path_olap_c, mode='a', header=False, index=False)

    state_mgr.save()
    print("\n--- PROCESO COMPLETADO ---")

if __name__ == "__main__":
    run_etl()