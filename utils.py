import logging
import pandas as pd
from pandas.errors import EmptyDataError

# Setup de logging
def setup_logging(level='INFO', log_file='migration.log'):
    log = logging.getLogger()
    log.setLevel(level)
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)
    return log

# Cargar archivo CSV
def load_csv(file_path, log):
    try:
        df = pd.read_csv(file_path, header=None)  # Asegúrate de usar header=None si no tienes cabecera
        log.info(f"Archivo CSV cargado correctamente desde: {file_path}")
        return df
    except EmptyDataError:
        log.error(f"El archivo CSV está vacío: {file_path}")
        return None
    except Exception as e:
        log.error(f"Error al cargar el archivo CSV: {str(e)}")
        return None

# Insertar en la base de datos por lotes
def insert_batch(model, data, db_session, log):
    try:
        db_session.bulk_insert_mappings(model, data)
        db_session.commit()
        log.info(f"{len(data)} registros insertados en la tabla {model.__tablename__}")
    except Exception as e:
        db_session.rollback()
        log.error(f"Error al insertar registros en {model.__tablename__}: {str(e)}")
        raise
