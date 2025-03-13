from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# Tabla Department
class Department(db.Model):
    __tablename__ = 'departments'
    id = db.Column(db.Integer, primary_key=True)
    department = db.Column(db.String(255), nullable=False, unique=True)

# Tabla Job
class Job(db.Model):
    __tablename__ = 'jobs'
    id = db.Column(db.Integer, primary_key=True)
    job = db.Column(db.String(255), nullable=False, unique=True)

# Tabla HiredEmployee
class HiredEmployee(db.Model):
    __tablename__ = 'hired_employees'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    hire_datetime = db.Column(db.DateTime, nullable=False) 
    department_id = db.Column(db.Integer, db.ForeignKey('departments.id'), nullable=False)
    job_id = db.Column(db.Integer, db.ForeignKey('jobs.id'), nullable=True)  

    department = db.relationship('Department', backref=db.backref('employees', lazy=True))
    job = db.relationship('Job', backref=db.backref('employees', lazy=True))
