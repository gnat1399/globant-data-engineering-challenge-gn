# Globant's Data Engineering Coding Challenge 2025

## Descripción

Este proyecto cumple con el reto de Data Engineering que consiste en:
1. Recibir datos históricos de archivos CSV (3 tablas: `departments`, `jobs`, `hired_employees`).
2. Subir estos archivos a una base de datos SQL.
3. Permitir la inserción de transacciones por lotes (de 1 a 1000 filas) con una sola solicitud.

## Estructura del Proyecto

.
├── app.py                # Archivo principal que define la API Flask
├── models.py             # Definición de los modelos (departments, jobs, hired_employees)
├── utils.py              # Funciones auxiliares (load_csv, insert_batch, etc.)
├── config.py             # Configuración de la base de datos y variables de entorno
├── requirements.txt      # Lista de dependencias para Python
├── README.md             # Archivo de documentación (este archivo)
└── ...


### Archivos Clave

- **`app.py`**  
  Contiene la configuración de Flask y los endpoints principales:
  - `POST /upload_csv` para recibir los archivos CSV y procesarlos.
  - Endpoints de reporte (`/report/quarterly-hired`, `/report/departments-above-average`) para consultas adicionales.

- **`models.py`**  
  Define las tablas `departments`, `jobs` y `hired_employees` usando SQLAlchemy:
  - `Department`
  - `Job`
  - `HiredEmployee`

- **`utils.py`**  
  Funciones auxiliares:
  - `load_csv`: Carga y preprocesa el CSV con pandas.
  - `insert_batch`: Inserta registros en la base de datos y maneja IDs duplicados.
  - `setup_logging`: Configura el logging para registrar eventos.

- **`config.py`**  
  Contiene la variable `SQLALCHEMY_DATABASE_URI` y otras configuraciones, como la ruta de la base de datos (`migracion.db` o `migration.db`).

## Requisitos

- **Python 3.8+**  
- **SQLite** (u otra base de datos SQL, aunque aquí se configura SQLite por defecto).
- **Pandas** y **SQLAlchemy** para la lectura de CSV y la manipulación de datos en la base de datos.

Para instalar las dependencias, ejecuta:

```bash
pip install -r requirements.txt 
```

## Cómo Ejecutar el Proyecto

- **Clonar este repositorio**  
  ```bash
  git clone https://github.com/gnat1399/globant-data-engineering-challenge-gn.git
  cd globant-data-engineering-challenge-gn

- **Instalar las dependencias**

```bash
pip install -r requirements.txt
Ejecutar la aplicación Flask
```
```bash
python app.py
```
Verás un mensaje en la consola, por ejemplo: Iniciando servidor Flask en http://127.0.0.1:5000/

- **Probar los endpoints principales**

Subir archivos CSV
POST /upload_csv
En el form-data, envía los archivos:
file_departments (CSV de departamentos)
file_hired_employees (CSV de empleados contratados)
file_jobs (CSV de puestos)

-Reporte 1
GET /report/quarterly-hired
Devuelve la cantidad de empleados contratados en 2021 por departamento y trabajo, dividido por trimestres (Q1, Q2, Q3, Q4).
-Reporte 2
GET /report/departments-above-average
Devuelve los departamentos que contrataron más empleados que el promedio de todos los departamentos en 2021.
Flujo de Ejecución
Cargar CSV
El endpoint POST /upload_csv recibe los archivos y los guarda localmente en la carpeta configurada (UPLOAD_FOLDER).

Procesamiento con pandas
En utils.py, la función load_csv lee el CSV, aplica limpieza de datos y lo convierte en un DataFrame.

Inserción en la base de datos

Se revisan duplicados con insert_batch, y en caso de conflicto de ID, se actualiza o se inserta.
Para la tabla hired_employees, se parsean las fechas con el formato ISO (YYYY-MM-DDTHH:MM:SSZ). Si alguna fecha es inválida, se ignora esa fila (ver lógica en app.py).
Consultas / Reportes

/report/quarterly-hired: Extrae el trimestre con extract('month', ...) y agrupa por departamento/trabajo.
/report/departments-above-average: Calcula el promedio de contrataciones y filtra los departamentos que superan ese promedio.