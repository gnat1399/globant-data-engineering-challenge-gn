import os
from flask import Flask, request, jsonify
from models import db, Department, HiredEmployee, Job
from config import SQLALCHEMY_DATABASE_URI, LOGGING_LEVEL, LOGGING_FILE, UPLOAD_FOLDER
from utils import setup_logging, load_csv, insert_batch

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

db.init_app(app)

# Setup de logging
log = setup_logging(LOGGING_LEVEL, LOGGING_FILE)

# Ruta raíz para verificar que el servidor funciona
@app.route('/')
def home():
    return "¡El servidor está funcionando correctamente!"

# Ruta para la carga de los archivos CSV
@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    # Obtener los tres archivos de la solicitud
    file_departments = request.files.get('file_departments')
    file_hired_employees = request.files.get('file_hired_employees')
    file_jobs = request.files.get('file_jobs')

    # Verificar que se han recibido los 3 archivos
    if not file_departments or not file_hired_employees or not file_jobs:
        log.error('Faltan archivos en la solicitud')
        return jsonify({"error": "Faltan archivos en la solicitud"}), 400

    # Guardar los archivos CSV
    file_path_departments = os.path.join(app.config['UPLOAD_FOLDER'], file_departments.filename)
    file_path_hired_employees = os.path.join(app.config['UPLOAD_FOLDER'], file_hired_employees.filename)
    file_path_jobs = os.path.join(app.config['UPLOAD_FOLDER'], file_jobs.filename)

    file_departments.save(file_path_departments)
    file_hired_employees.save(file_path_hired_employees)
    file_jobs.save(file_path_jobs)

    # Procesar los archivos CSV
    df_departments = load_csv(file_path_departments, log)
    df_hired_employees = load_csv(file_path_hired_employees, log)
    df_jobs = load_csv(file_path_jobs, log)

    if df_departments is None or df_hired_employees is None or df_jobs is None:
        return jsonify({"error": "Error al procesar uno o más archivos CSV"}), 500

    # Insertar los datos en la base de datos por lotes
    # Insertar departamentos
    data_departments = [{"id": row['id'], "department": row['department']} for _, row in df_departments.iterrows()]
    insert_batch(Department, data_departments, db.session, log)

    # Insertar empleados contratados
    data_hired_employees = [{"id": row['id'], "name": row['name'], "hire_datetime": row['datetime'], "department_id": row['department_id'], "job_id": row['job_id']} for _, row in df_hired_employees.iterrows()]
    insert_batch(HiredEmployee, data_hired_employees, db.session, log)

    # Insertar trabajos
    data_jobs = [{"id": row['id'], "job": row['job']} for _, row in df_jobs.iterrows()]
    insert_batch(Job, data_jobs, db.session, log)

    return jsonify({"message": "Archivos procesados e insertados correctamente"}), 200

# Ruta para inserción por lotes
@app.route('/insert_batch', methods=['POST'])
def insert_batch_data():
    data = request.json
    if not data:
        log.error('No data provided for batch insert')
        return jsonify({"error": "No data provided"}), 400

    for item in data:
        if item['tabla'] == 'departments':
            insert_batch(Department, item['data'], db.session, log)
        elif item['tabla'] == 'hired_employees':
            insert_batch(HiredEmployee, item['data'], db.session, log)
        elif item['tabla'] == 'jobs':
            insert_batch(Job, item['data'], db.session, log)
        else:
            log.error(f"Unknown table: {item['tabla']}")
            return jsonify({"error": f"Unknown table: {item['tabla']}"}), 400

    return jsonify({"message": "Batch inserted successfully"}), 200

if __name__ == '__main__':
    print("Iniciando servidor Flask...")
    app.run(debug=True, port=5000)  # Inicia el servidor en el puerto 5000
