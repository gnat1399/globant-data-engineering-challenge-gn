from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

# Configuracion de la base de datos
DATABASE_URL = "sqlite:///migracion.db"

# Crear una instancia para definir las tablas
Base = declarative_base()

# Crear el motor de base de datos y la sesión
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

from models import Department, Job, HiredEmployee
from database import engine

# Crear tablas
Base.metadata.create_all(engine)