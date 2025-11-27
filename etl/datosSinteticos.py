import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

# --- CONFIGURACIÓN DE ESCALA ---
fake = Faker('es_MX')
NUM_CLIENTES = 20       # MODIFICADO: Solo 20 clientes
NUM_EMPLEADOS = 120     # Más empleados para cubrir la rotación de 10 años
NUM_PROYECTOS = 500     # Requerimiento solicitado
YEARS_HISTORY = 10      # Ventana de tiempo

# --- FUNCIONES AUXILIARES ---
def date_between(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    if int_delta <= 0: return start
    random_second = random.randrange(int_delta)
    return start + timedelta(seconds=random_second)

def determinar_estado_logico(fecha_inicio):
    # Regla: Si el proyecto inició hace más de 1 año, NO puede estar "en curso" o "planificado"
    hoy = datetime.now().date()
    antiguedad_dias = (hoy - fecha_inicio).days
    
    if antiguedad_dias > 365:
        return random.choices(['finalizado', 'cancelado'], weights=[0.90, 0.10])[0]
    else:
        # Proyectos recientes pueden tener cualquier estado
        return random.choices(['finalizado', 'en curso', 'planificado', 'cancelado'], 
                              weights=[0.30, 0.50, 0.15, 0.05])[0]

print("--- 1. Generando Clientes... ---")
# MODIFICADO: Generar un pool fijo de solo 10 países
paises_disponibles = set()
while len(paises_disponibles) < 10:
    paises_disponibles.add(fake.country())
paises_disponibles = list(paises_disponibles)

data_clientes = []
for i in range(1, NUM_CLIENTES + 1):
    data_clientes.append({
        'id_cliente': i,
        'nombre_cliente': fake.company(),
        'correo': fake.company_email(),
        'telefono': fake.phone_number(),
        'industria': fake.job(),
        'pais': random.choice(paises_disponibles), # MODIFICADO: Seleccionar del pool de 10
        'ciudad': fake.city(),
        'nivel_satisfaccion': random.randint(2, 5) # Variedad en satisfacción
    })
pd.DataFrame(data_clientes).to_csv('csv/clientes.csv', index=False)

print("--- 2. Generando Metodologías... ---")
metodologias = ['Scrum', 'Kanban', 'Waterfall', 'XP', 'DevOps']
data_metodos = [{'id_metodologia': i+1, 'nombre_metodologia': m, 'documentacion': f'Doc v{random.randint(1,5)}'} for i, m in enumerate(metodologias)]
pd.DataFrame(data_metodos).to_csv('csv/metodologias.csv', index=False)

print("--- 3. Generando Empleados (Con antigüedad histórica)... ---")
roles = ['Dev Backend', 'Dev Frontend', 'QA Engineer', 'Project Manager', 'DevOps', 'Data Scientist']
data_empleados = []
empleados_info = {} # Diccionario para consultar fechas de contratación rápido

for i in range(1, NUM_EMPLEADOS + 1):
    # Contrataciones distribuidas en los últimos 12 años (algunos muy antiguos)
    fecha_contratacion = fake.date_between(start_date=f'-{YEARS_HISTORY + 2}y', end_date='today')
    
    # Determinar si sigue activo (rotación de personal)
    if fecha_contratacion.year < (datetime.now().year - 5):
        estado_laboral = random.choices(['activo', 'inactivo'], weights=[0.6, 0.4])[0]
    else:
        estado_laboral = 'activo'

    data_empleados.append({
        'id_empleado': i,
        'nombre_completo': fake.name(),
        'rol': random.choice(roles),
        'salario_hora': round(random.uniform(30.0, 120.0), 2),
        'disponibilidad': 1 if estado_laboral == 'activo' else 0,
        'fecha_contratacion': fecha_contratacion,
        'estado_laboral': estado_laboral,
        'habilidades': fake.job()
    })
    
    empleados_info[i] = {
        'fecha_contratacion': fecha_contratacion,
        'estado_actual': estado_laboral
    }
pd.DataFrame(data_empleados).to_csv('csv/empleados.csv', index=False)

print("--- 4. Generando Proyectos (Lógica Temporal)... ---")
data_proyectos = []
proyectos_info = {} 

for i in range(1, NUM_PROYECTOS + 1):
    # Fecha inicio distribuida en los últimos 10 años
    fecha_inicio_est = fake.date_between(start_date=f'-{YEARS_HISTORY}y', end_date='today')
    
    # Determinar estado basado en la fecha
    estado = determinar_estado_logico(fecha_inicio_est)
    
    # Duración: Proyectos chicos (1 mes) a grandes (1 año)
    duracion_est_dias = random.randint(30, 365)
    fecha_fin_est = fecha_inicio_est + timedelta(days=duracion_est_dias)
    
    # Realidad vs Estimación
    fecha_inicio_real = fecha_inicio_est + timedelta(days=random.randint(-5, 10))
    
    if estado == 'finalizado':
        # Factor de retraso (algunos terminan antes 0.9, otros tarde 1.4)
        factor_retraso = random.uniform(0.9, 1.4)
        duracion_real = int(duracion_est_dias * factor_retraso)
        fecha_fin_real = fecha_inicio_real + timedelta(days=duracion_real)
    elif estado == 'cancelado':
        # Se cancela a mitad de camino
        fecha_fin_real = fecha_inicio_real + timedelta(days=random.randint(10, duracion_est_dias))
    else:
        # En curso o planificado
        fecha_fin_real = None

    # Finanzas
    # Costo hora base sube con los años (inflación simulada)
    year_factor = (fecha_inicio_real.year - (datetime.now().year - 10)) * 2 
    costo_promedio = 40 + year_factor 
    
    horas_est = duracion_est_dias * 8 * random.randint(3, 8) # Equipo de 3 a 8 personas
    presupuesto_est = horas_est * costo_promedio
    valor_venta = presupuesto_est * random.uniform(1.3, 1.6) # Margen 30-60%

    horas_reales = 0
    presupuesto_real = 0
    ganancias = 0

    if estado == 'finalizado':
        horas_reales = int(horas_est * random.uniform(0.9, 1.3))
        presupuesto_real = horas_reales * costo_promedio
        ganancias = valor_venta - presupuesto_real

    data_proyectos.append({
        'id_proyecto': i,
        'nombre_proyecto': f"Proy-{fecha_inicio_est.strftime('%Y')}-{fake.bs().split()[0]}", # Nombres tipo "Proy-2023-Synergy"
        'id_cliente': random.randint(1, NUM_CLIENTES), # Usa el nuevo límite de 20
        'fecha_inicio_estimada': fecha_inicio_est,
        'fecha_fin_estimada': fecha_fin_est,
        'fecha_inicio_real': fecha_inicio_real,
        'fecha_fin_real': fecha_fin_real,
        'estado': estado,
        'presupuesto_estimado': round(presupuesto_est, 2),
        'presupuesto_real': round(presupuesto_real, 2),
        'valor_venta': round(valor_venta, 2),
        'ganancias': round(ganancias, 2),
        'horas_estimadas': horas_est,
        'horas_totales': horas_reales,
        'prioridad': random.choice(['alta', 'media', 'baja']),
        'id_metodologia': random.randint(1, len(metodologias))
    })
    
    proyectos_info[i] = {
        'start': fecha_inicio_real,
        'end': fecha_fin_real if fecha_fin_real else datetime.now().date(),
        'status': estado
    }
pd.DataFrame(data_proyectos).to_csv('csv/proyectos.csv', index=False)

print("--- 5. Generando Asignaciones (Validando contratación)... ---")
data_asignaciones = []
id_asig_counter = 1
asig_to_project_map = {} 

for p_id in range(1, NUM_PROYECTOS + 1):
    p_start = proyectos_info[p_id]['start']
    
    # FILTRO IMPORTANTE: Solo empleados contratados ANTES de que inicie el proyecto
    empleados_disponibles = [
        emp_id for emp_id, info in empleados_info.items() 
        if info['fecha_contratacion'] <= p_start 
        and (info['estado_actual'] == 'activo' or p_start.year < datetime.now().year)
    ]
    
    # Si no hay empleados antiguos (raro, pero posible al inicio), tomamos los primeros
    if not empleados_disponibles:
        empleados_disponibles = list(range(1, 10))

    equipo_size = random.randint(4, 10)
    # Evitar error si el equipo es más grande que los disponibles
    equipo_size = min(equipo_size, len(empleados_disponibles))
    
    seleccionados = random.sample(empleados_disponibles, equipo_size)
    
    for emp_id in seleccionados:
        rol_asig = 'Tester' if random.random() < 0.3 else 'Developer' # 30% Testers
        
        data_asignaciones.append({
            'id_asignacion': id_asig_counter,
            'id_empleado': emp_id,
            'id_proyecto': p_id,
            'rol': rol_asig,
            'fecha_asignacion': p_start,
            'estado': 'finalizado' if proyectos_info[p_id]['status'] == 'finalizado' else 'activo'
        })
        asig_to_project_map[id_asig_counter] = {'p_id': p_id, 'rol': rol_asig}
        id_asig_counter += 1

pd.DataFrame(data_asignaciones).to_csv('csv/asignaciones.csv', index=False)

print("--- 6. Generando Tareas y Pruebas (Rayleigh)... ---")
data_tareas = []
data_pruebas = []
id_tarea_c = 1
id_prueba_c = 1

for asig_id, info in asig_to_project_map.items():
    p_id = info['p_id']
    rol = info['rol']
    p_data = proyectos_info[p_id]
    
    # Fechas límite para tareas/pruebas
    start_date = p_data['start']
    end_date = p_data['end']
    # Si el proyecto sigue en curso, la fecha tope es hoy
    if not isinstance(end_date, datetime) and end_date > datetime.now().date():
         fecha_tope_logica = datetime.now().date()
    else:
         fecha_tope_logica = end_date

    dias_proyecto = (fecha_tope_logica - start_date).days
    if dias_proyecto < 1: dias_proyecto = 1

    # Generar TAREAS (para todos)
    num_tareas = random.randint(3, 8)
    for _ in range(num_tareas):
        # Distribución uniforme para tareas (trabajo constante)
        dia_random = random.randint(0, dias_proyecto)
        f_asig = start_date + timedelta(days=dia_random)
        f_lim = f_asig + timedelta(days=5)
        
        estado_t = 'completada' if p_data['status'] == 'finalizado' else 'pendiente'

        data_tareas.append({
            'id_tarea': id_tarea_c,
            'id_asignacion': asig_id,
            'nombre': f"Tarea genérica {id_tarea_c}",
            'fecha_asignacion': f_asig,
            'fecha_limite': f_lim,
            'estado': estado_t,
            'tiempo_estimado': 5.0,
            'tiempo_real': 5.5,
            'prioridad': 'media'
        })
        id_tarea_c += 1

    # Generar PRUEBAS (Solo si es Tester y el proyecto avanzó)
    if rol == 'Tester' and dias_proyecto > 10:
        # Un tester hace muchas pruebas
        num_pruebas = random.randint(20, 60)
        
        for _ in range(num_pruebas):
            # --- LOGICA RAYLEIGH (Beta 5,2) ---
            # Alpha 5, Beta 2 sesga la curva hacia la derecha (final del proyecto)
            avance_pct = random.betavariate(5, 2) 
            
            dia_ejec = int(dias_proyecto * avance_pct)
            # Asegurar que no exceda el tiempo actual
            if dia_ejec >= dias_proyecto: dia_ejec = dias_proyecto - 1
            
            f_ejec = start_date + timedelta(days=dia_ejec)
            
            # Probabilidad de fallo
            es_fallo = random.random() < 0.2
            res = 'fallo' if es_fallo else 'éxito'
            sev = random.choice(['baja', 'media', 'alta', 'crítica']) if es_fallo else 'nula'
            
            data_pruebas.append({
                'id_prueba': id_prueba_c,
                'id_asignacion': asig_id,
                'descripcion': f"Prueba auto {id_prueba_c}",
                'tipo': random.choice(['sistema', 'integración']),
                'fecha_ejecucion': f_ejec,
                'resultado': res,
                'severidad_defecto': sev,
                'tiempo_resolucion_horas': random.randint(1, 8) if es_fallo else 0
            })
            id_prueba_c += 1

pd.DataFrame(data_tareas).to_csv('csv/tareas.csv', index=False)
pd.DataFrame(data_pruebas).to_csv('csv/pruebas.csv', index=False)

print("--- 7. Generando Finanzas... ---")
data_finanzas = []
for i in range(1, NUM_PROYECTOS + 1):
    data_finanzas.append({
        'id_finanza': i,
        'id_proyecto': i,
        'tipo_gasto': 'infraestructura',
        'costo_estimado': 1000,
        'costo_real': 1200,
        'fecha_registro': proyectos_info[i]['start']
    })
pd.DataFrame(data_finanzas).to_csv('csv/finanzas.csv', index=False)

print(f"¡Generación masiva completada! {NUM_PROYECTOS} proyectos para {NUM_CLIENTES} clientes en 10 países.")