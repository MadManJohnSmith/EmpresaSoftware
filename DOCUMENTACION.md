# Documentación del Sistema DSS

## 1. Procesos ETL (Extracción, Transformación y Carga)

El sistema se alimenta de datos generados y procesados mediante scripts en Python, siguiendo un enfoque ROLAP (Relational OLAP) simplificado con archivos CSV.

### Flujo de Datos
1.  **Generación de Datos**: El script `etl/datosSinteticos.py` genera datos simulados de proyectos, transacciones financieras y registros de calidad.
2.  **Procesamiento ETL**: El script `etl/ETL.py` toma los datos crudos, realiza limpiezas, agregaciones y cálculos de KPIs.
3.  **Almacenamiento**: Los datos procesados se guardan en `OLAP_Proyectos.csv` y `OLAP_Calidad.csv`, que actúan como nuestro Data Warehouse ligero.

### Ejecución
Para regenerar los datos:
```bash
python etl/datosSinteticos.py
python etl/ETL.py
```

## 2. Generación de Analíticas (Balanced Scorecard)

El Dashboard implementa un Balanced Scorecard con las siguientes métricas:

### Perspectiva Financiera
-   **Ganancia Neta**: Ingresos - Costos.
-   **Margen de Beneficio**: (Ganancia / Ventas) * 100.
-   **Costo Operativo**: Suma de costos reales de proyectos.

### Perspectiva del Cliente
-   **Top Clientes**: Ranking de clientes por rentabilidad.
-   **Mapa de Calor**: Distribución geográfica de ganancias.

### Perspectiva de Procesos (Calidad)
-   **Defectos Totales**: Suma de defectos encontrados.
-   **Defectos Críticos**: Defectos con severidad alta.

## 3. Modelo Predictivo (Rayleigh)

El sistema incluye un módulo avanzado para la predicción de defectos en software, exclusivo para Project Managers.

### Metodología
1.  **Simulación Montecarlo**: Utiliza la media y desviación estándar histórica de defectos para simular 5,000+ escenarios posibles para el próximo proyecto.
2.  **Curva de Rayleigh**: Proyecta la distribución de hallazgo de defectos en el tiempo usando la fórmula:
    $$ f(t) = \frac{2K}{t_m} \frac{t}{t_m} e^{-(t/t_m)^2} $$
    Donde $K$ es el total de defectos esperados (de la simulación) y $t_m$ es el momento pico de defectos.

### Puesta en Producción
El modelo está integrado en tiempo real en `app.py`. Al ajustar los parámetros (nivel de confianza, número de simulaciones), el PM obtiene instantáneamente la proyección de recursos necesarios para QA.

## 4. Evidencias de Trabajo en Equipo

*(Espacio reservado para capturas de pantalla de commits, reuniones o tablero de tareas)*

## 5. Aportaciones Individuales

*(Espacio reservado para describir roles específicos de cada integrante)*
