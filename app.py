import os
from flask import Flask, request, jsonify
from models import db, Department, HiredEmployee, Job
from config import SQLALCHEMY_DATABASE_URI, LOGGING_LEVEL, LOGGING_FILE, UPLOAD_FOLDER
from utils import setup_logging, load_csv, insert_batch
from datetime import datetime

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

db.init_app(app)

# Configurar logging sin emojis para evitar errores en Windows
log = setup_logging(LOGGING_LEVEL, LOGGING_FILE)

with app.app_context():
    db.create_all()

@app.route('/')
def home():
    return "¡El servidor Flask está funcionando correctamente!"

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    try:
        log.info("📂 Recibiendo archivos CSV...")

        # Obtener archivos
        file_departments = request.files.get('file_departments')
        file_hired_employees = request.files.get('file_hired_employees')
        file_jobs = request.files.get('file_jobs')

        if not file_departments or not file_hired_employees or not file_jobs:
            log.error('[ERROR] Faltan archivos en la solicitud.')
            return jsonify({"error": "Faltan archivos en la solicitud"}), 400

        # Guardar archivos CSV
        file_paths = {
            "departments": os.path.join(app.config['UPLOAD_FOLDER'], "departments.csv"),
            "hired_employees": os.path.join(app.config['UPLOAD_FOLDER'], "hired_employees.csv"),
            "jobs": os.path.join(app.config['UPLOAD_FOLDER'], "jobs.csv")
        }

        file_departments.save(file_paths["departments"])
        file_hired_employees.save(file_paths["hired_employees"])
        file_jobs.save(file_paths["jobs"])

        log.info(f"[✔] Archivos guardados en: {file_paths}")

        # Procesar archivos CSV con nombres de columna definidos
        df_departments = load_csv(file_paths["departments"], log, ["id", "department"])
        df_hired_employees = load_csv(file_paths["hired_employees"], log, ["id", "name", "datetime", "department_id", "job_id"])
        df_jobs = load_csv(file_paths["jobs"], log, ["id", "job"])

        if df_departments is None or df_hired_employees is None or df_jobs is None:
            return jsonify({"error": "Error al procesar uno o más archivos CSV"}), 500

        insert_data(df_departments, df_hired_employees, df_jobs)

        return jsonify({"message": "Datos insertados correctamente"}), 200

    except Exception as e:
        log.error(f"[ERROR] {str(e)}")
        return jsonify({"error": str(e)}), 500

def insert_data(df_departments, df_hired_employees, df_jobs):
    """ Inserta los datos en la base de datos en lotes """
    data_departments = [{"id": int(row['id']), "department": row['department']} for _, row in df_departments.iterrows()]
    insert_batch(Department, data_departments, db.session, log)

    data_jobs = [{"id": int(row['id']), "job": row['job']} for _, row in df_jobs.iterrows()]
    insert_batch(Job, data_jobs, db.session, log)

    data_hired_employees = []
    for _, row in df_hired_employees.iterrows():
        try:
            hire_date = datetime.strptime(row['datetime'], "%Y-%m-%dT%H:%M:%SZ")
            data_hired_employees.append({
                "id": int(row['id']),
                "name": row['name'],
                "hire_datetime": hire_date,
                "department_id": int(row['department_id']),
                "job_id": int(row['job_id'])
            })
        except ValueError as e:
            log.error(f"[ERROR] Formato de fecha incorrecto en {row['datetime']}: {str(e)}")
            raise ValueError(f"Formato de fecha incorrecto: {row['datetime']}")

    insert_batch(HiredEmployee, data_hired_employees, db.session, log)

if __name__ == '__main__':
    print("Iniciando servidor Flask en http://127.0.0.1:5000/")
    app.run(debug=True, port=5000)
