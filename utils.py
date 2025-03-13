import logging
import pandas as pd
from pandas.errors import EmptyDataError
from sqlalchemy.exc import IntegrityError

def setup_logging(level='INFO', log_file='migration.log'):
    """ Configura el logging para registrar eventos en archivo y consola. """
    log = logging.getLogger(__name__)
    log.setLevel(getattr(logging, level.upper(), logging.INFO))

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('[%(levelname)s] %(message)s')
    console_handler.setFormatter(console_formatter)

    log.addHandler(file_handler)
    log.addHandler(console_handler)

    return log

def load_csv(file_path, log, column_names=None, date_columns=None):
    """
    Carga un archivo CSV y lo devuelve como un DataFrame con las fechas convertidas.
    Si ocurre error en la conversión de alguna fecha, se asigna None a esa celda,
    pero NO se descarta la fila. La lógica de ignorar filas defectuosas puede
    manejarse posteriormente (por ejemplo, en app.py).
    
    :param file_path: Ruta del archivo CSV.
    :param log: Objeto de logging.
    :param column_names: Lista opcional de nombres de columna.
    :param date_columns: Lista de columnas que contienen fechas.
    :return: DataFrame o None si hay un error.
    """
    try:
        df = pd.read_csv(file_path, header=None, encoding='utf-8')
        if column_names and len(column_names) == df.shape[1]:
            df.columns = column_names
        
        if date_columns:
            # Función auxiliar para convertir cada valor de fecha
            def convert_date(x, col):
                s = str(x).strip(" ,")
                if s == "":
                    return None  # No se puede parsear
                try:
                    # Ajustar formato según tu CSV (por ejemplo '%Y-%m-%dT%H:%M:%SZ')
                    return pd.to_datetime(s, format='%Y-%m-%dT%H:%M:%SZ', errors='raise')
                except Exception as e:
                    log.error(f"Error converting value in column '{col}': '{s}' -> {e}")
                    return None

            for col in date_columns:
                if col in df.columns:
                    # Convertir a string y limpiar espacios y comas
                    df[col] = df[col].astype(str).str.strip(" ,")
                    log.info(f"Valores únicos en '{col}' tras limpiar: {df[col].unique()}")
                    # Aplicar la conversión individualmente
                    df[col] = df[col].apply(lambda x: convert_date(x, col))
                    # Convertir la fecha a string con formato estándar (YYYY-MM-DD HH:MM:SS)
                    # Si es None, se quedará como None
                    df[col] = df[col].apply(
                        lambda d: d.strftime('%Y-%m-%d %H:%M:%S') if d is not None and pd.notnull(d) else None
                    )
        
        # Eliminar filas completamente vacías
        df.dropna(how='all', inplace=True)
        # Reemplaza NaN con cadena vacía
        df.fillna(value="", inplace=True)
        
        log.info(f"[✔] Archivo CSV cargado correctamente: {file_path} ({len(df)} filas)")
        return df
    
    except FileNotFoundError:
        log.error(f"[ERROR] El archivo no existe: {file_path}")
    except EmptyDataError:
        log.error(f"[ERROR] El archivo CSV está vacío: {file_path}")
    except UnicodeDecodeError:
        log.error(f"[ERROR] Error de codificación al leer el archivo: {file_path}. Intenta abrirlo en UTF-8.")
    except Exception as e:
        log.error(f"[ERROR] No se pudo cargar el archivo CSV: {str(e)}")
    
    return None

def insert_batch(model, data, db_session, log):
    """
    Inserta registros en la base de datos y maneja IDs duplicados.
    Después de la inserción, consulta y muestra los registros insertados.
    :param model: Modelo SQLAlchemy.
    :param data: Lista de diccionarios con datos a insertar.
    :param db_session: Sesión SQLAlchemy.
    :param log: Objeto de logging.
    """
    if not data:
        log.warning(f"[⚠] No hay datos para insertar en {model.__tablename__}.")
        return
    
    try:
        db_session.bulk_insert_mappings(model, data)
        db_session.commit()
        log.info(f"[✔] {len(data)} registros insertados en {model.__tablename__}")
        
        # Consulta para verificar los registros insertados
        registros = db_session.query(model).all()
        log.info(f"[✔] Registros en {model.__tablename__}:")
        for r in registros:
            registro_dict = r.__dict__.copy()
            registro_dict.pop('_sa_instance_state', None)
            log.info(str(registro_dict))
            
    except IntegrityError:
        db_session.rollback()
        log.warning(f"[⚠] Conflicto de ID detectado en {model.__tablename__}. Intentando actualización...")
        
        for record in data:
            existing_record = db_session.get(model, record['id'])
            if existing_record:
                for key, value in record.items():
                    setattr(existing_record, key, value)
                log.info(f"[✔] Registro con ID {record['id']} actualizado en {model.__tablename__}.")
            else:
                db_session.add(model(**record))
        
        db_session.commit()
        log.info(f"[✔] Registros duplicados en {model.__tablename__} han sido actualizados o insertados correctamente.")
        
        # Consulta nuevamente para verificar la actualización
        registros = db_session.query(model).all()
        log.info(f"[✔] Registros en {model.__tablename__} después de la actualización:")
        for r in registros:
            registro_dict = r.__dict__.copy()
            registro_dict.pop('_sa_instance_state', None)
            log.info(str(registro_dict))
            
    except Exception as e:
        db_session.rollback()
        log.error(f"[ERROR] Fallo al insertar en {model.__tablename__}: {str(e)}")
        raise

def count_records(model, db_session, log):
    """
    Cuenta la cantidad de registros en una tabla dada.
    :param model: Modelo SQLAlchemy.
    :param db_session: Sesión SQLAlchemy.
    :param log: Objeto de logging.
    """
    try:
        count = db_session.query(model).count()
        log.info(f"[✔] La tabla {model.__tablename__} contiene {count} registros.")
        return count
    except Exception as e:
        log.error(f"[ERROR] No se pudo contar los registros en {model.__tablename__}: {str(e)}")
        return None
