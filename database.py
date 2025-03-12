from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

# Configuración de la base de datos SQLite
DATABASE_URL = "sqlite:///migracion.db"

# Crear una instancia de base para definir las tablas
Base = declarative_base()

# Crear el motor de base de datos y la sesión
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

from models import Department, Job, HiredEmployee
from database import engine

# Crear las tablas en la base de datos
Base.metadata.create_all(engine)