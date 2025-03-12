from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# Tabla para Departamentos
class Department(db.Model):
    __tablename__ = 'departments'
    id = db.Column(db.Integer, primary_key=True)
    department = db.Column(db.String(255), nullable=False)

# Tabla para Empleados
class HiredEmployee(db.Model):
    __tablename__ = 'hired_employees'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    hire_datetime = db.Column(db.String(50), nullable=False)  # Guardamos la fecha en formato ISO
    department_id = db.Column(db.Integer, db.ForeignKey('departments.id'), nullable=False)
    job_id = db.Column(db.Integer, db.ForeignKey('jobs.id'), nullable=False)  # Referencia a Job

    department = db.relationship('Department', backref=db.backref('employees', lazy=True))
    job = db.relationship('Job', backref=db.backref('employees', lazy=True))

# Tabla para Trabajos
class Job(db.Model):
    __tablename__ = 'jobs'
    id = db.Column(db.Integer, primary_key=True)
    job = db.Column(db.String(255), nullable=False)
