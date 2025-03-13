import os
from flask import Flask, request, jsonify
from sqlalchemy import func, case, extract
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
    # 1) Insertar departamentos
    data_departments = [{"id": int(row['id']), "department": row['department']} for _, row in df_departments.iterrows()]
    insert_batch(Department, data_departments, db.session, log)

    # 2) Insertar jobs
    data_jobs = [{"id": int(row['id']), "job": row['job']} for _, row in df_jobs.iterrows()]
    insert_batch(Job, data_jobs, db.session, log)

    # 3) Insertar hired_employees
    data_hired_employees = []
    for _, row in df_hired_employees.iterrows():
        try:
            # Ajusta el formato a tu caso real: 
            # Este formato '%Y-%m-%dT%H:%M:%SZ' maneja '2021-05-30T05:43:46Z'
            hire_date = datetime.strptime(row['datetime'], "%Y-%m-%dT%H:%M:%SZ")

            data_hired_employees.append({
                "id": int(row['id']),
                "name": row['name'],
                "hire_datetime": hire_date,
                "department_id": int(row['department_id']),
                "job_id": int(row['job_id'])
            })

        except ValueError as e:
            # En lugar de detener el proceso, logeamos y continuamos
            log.error(f"[ERROR] Formato de fecha incorrecto en {row['datetime']}: {str(e)}")
            log.error("[INFO] Se ignora esta fila y se continúa con el resto.")
            continue  # Saltar a la siguiente fila

    # Inserción por lotes de los empleados
    insert_batch(HiredEmployee, data_hired_employees, db.session, log)

# --------------------------------------------------------------------------
# NUEVA SECCIÓN: ENDPOINTS PARA REQUISITOS DE LA SECCIÓN 2
# --------------------------------------------------------------------------

@app.route('/report/quarterly-hired', methods=['GET'])
def quarterly_hired():
    """
    Endpoint que devuelve la cantidad de empleados contratados en 2021
    por departamento y trabajo, dividido por trimestres (Q1, Q2, Q3, Q4).
    """
    try:
        quarterly_data = (
            db.session.query(
                Department.department.label("department"),
                Job.job.label("job"),
                func.sum(
                    case(
                        [(extract('month', HiredEmployee.hire_datetime).in_([1, 2, 3]), 1)],
                        else_=0
                    )
                ).label("Q1"),
                func.sum(
                    case(
                        [(extract('month', HiredEmployee.hire_datetime).in_([4, 5, 6]), 1)],
                        else_=0
                    )
                ).label("Q2"),
                func.sum(
                    case(
                        [(extract('month', HiredEmployee.hire_datetime).in_([7, 8, 9]), 1)],
                        else_=0
                    )
                ).label("Q3"),
                func.sum(
                    case(
                        [(extract('month', HiredEmployee.hire_datetime).in_([10, 11, 12]), 1)],
                        else_=0
                    )
                ).label("Q4"),
            )
            .join(HiredEmployee, Department.id == HiredEmployee.department_id)
            .join(Job, Job.id == HiredEmployee.job_id)
            .filter(extract('year', HiredEmployee.hire_datetime) == 2021)
            .group_by(Department.department, Job.job)
            .order_by(Department.department.asc(), Job.job.asc())
            .all()
        )

        results = []
        for row in quarterly_data:
            results.append({
                "department": row.department,
                "job": row.job,
                "Q1": row.Q1,
                "Q2": row.Q2,
                "Q3": row.Q3,
                "Q4": row.Q4
            })

        return jsonify(results), 200

    except Exception as e:
        log.error(f"[ERROR] /report/quarterly-hired -> {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route('/report/departments-above-average', methods=['GET'])
def departments_above_average():
    """
    Endpoint que devuelve los departamentos (id, nombre, número de contratados)
    que contrataron más empleados que el promedio de todos los departamentos en 2021,
    ordenado de mayor a menor cantidad de contratados.
    """
    try:
        dept_hires_subquery = (
            db.session.query(
                Department.id.label("dept_id"),
                func.count(HiredEmployee.id).label("count_hires")
            )
            .join(HiredEmployee, Department.id == HiredEmployee.department_id)
            .filter(extract('year', HiredEmployee.hire_datetime) == 2021)
            .group_by(Department.id)
            .subquery()
        )

        average_hires = (
            db.session.query(func.avg(dept_hires_subquery.c.count_hires))
            .scalar()
        )

        dept_above_avg = (
            db.session.query(
                Department.id.label("id"),
                Department.department.label("name"),
                func.count(HiredEmployee.id).label("hired")
            )
            .join(HiredEmployee, Department.id == HiredEmployee.department_id)
            .filter(extract('year', HiredEmployee.hire_datetime) == 2021)
            .group_by(Department.id, Department.department)
            .having(func.count(HiredEmployee.id) > average_hires)
            .order_by(func.count(HiredEmployee.id).desc())
            .all()
        )

        results = []
        for row in dept_above_avg:
            results.append({
                "id": row.id,
                "department": row.name,
                "hired": row.hired
            })

        return jsonify(results), 200

    except Exception as e:
        log.error(f"[ERROR] /report/departments-above-average -> {str(e)}")
        return jsonify({"error": str(e)}), 500

# --------------------------------------------------------------------------

if __name__ == '__main__':
    print("Iniciando servidor Flask en http://127.0.0.1:5000/")
    app.run(debug=True, port=5000)
