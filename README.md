# Sistema de Soporte a Decisiones (DSS)

Este proyecto implementa un Sistema de Soporte a Decisiones (DSS) para una empresa de desarrollo de software. Incluye un Dashboard Dinámico, Balanced Scorecard y un Modelo Predictivo de Calidad basado en la distribución de Rayleigh.

## Características

- **Dashboard Dinámico**: Visualización de KPIs financieros, de clientes y de calidad.
- **Balanced Scorecard**: Implementación de las perspectivas Financiera, Cliente y Procesos Internos.
- **Modelo Predictivo**: Simulación Montecarlo y proyección de curva de Rayleigh para estimación de defectos.
- **Control de Acceso**: Sistema de login simple con Roles (Admin/PM/Invitado).

## Instalación

1.  Clonar el repositorio.
2.  Instalar las dependencias:
    ```bash
    pip install -r requirements.txt
    ```
3.  Ejecutar la aplicación:
    ```bash
    streamlit run app.py
    ```

## Credenciales de Acceso

Para acceder a las funcionalidades protegidas (Modelo Predictivo), utilice las siguientes credenciales:

| Rol | Usuario | Contraseña | Permisos |
| :--- | :--- | :--- | :--- |
| **Administrador** | `admin` | `admin123` | Acceso Total |
| **Project Manager** | `pm` | `pm123` | Acceso Total |
| **Invitado** | `invitado` | `guest` | Solo Dashboard y Scorecard |

## Estructura del Proyecto

- `app.py`: Aplicación principal (Streamlit).
- `etl/`: Scripts de Extracción, Transformación y Carga de datos.
- `OLAP_*.csv`: Archivos de datos procesados (Cubo ROLAP).
- `DOCUMENTACION.md`: Documentación detallada de procesos y modelos.
