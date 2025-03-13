import os

# Configuracion de la base de datos
SQLALCHEMY_DATABASE_URI = 'sqlite:///migration.db'  
SQLALCHEMY_TRACK_MODIFICATIONS = False 


# Configuracion de logging
LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO')
LOGGING_FILE = os.getenv('LOGGING_FILE', 'migration.log')

# Carpeta de carga de archivos CSV

BASE_DIR = os.path.abspath(os.path.dirname(__file__)) 
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads') 
