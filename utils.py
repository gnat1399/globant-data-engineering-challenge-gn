import logging
import pandas as pd
from pandas.errors import EmptyDataError
from sqlalchemy.exc import IntegrityError

def setup_logging(level='INFO', log_file='migration.log'):
    """ logging  """
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
    Carga un archivo CSV 
    """
    try:
        df = pd.read_csv(file_path, header=None, encoding='utf-8')
        if column_names and len(column_names) == df.shape[1]:
            df.columns = column_names
        
        if date_columns:
            # Función para convertir cada valor de fecha
            def convert_date(x, col):
                s = str(x).strip(" ,")
                if s == "":
                    return None
                try:
                    return pd.to_datetime(s, format='%Y-%m-%dT%H:%M:%SZ', errors='raise')
                except Exception as e:
                    log.error(f"Error converting value in column '{col}': '{s}' -> {e}")
                    return None

            for col in date_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip(" ,")
                    log.info(f"Valores únicos en '{col}' tras limpiar: {df[col].unique()}")
                    df[col] = df[col].apply(lambda x: convert_date(x, col))
                    df[col] = df[col].apply(
                        lambda d: d.strftime('%Y-%m-%d %H:%M:%S') if d is not None and pd.notnull(d) else None
                    )
        
        df.dropna(how='all', inplace=True)
        df.fillna(value="", inplace=True)
        
        log.info(f"Archivo  cargado : {file_path} ({len(df)} filas)")
        return df
    
    except FileNotFoundError:
        log.error(f"Archivo no existe: {file_path}")
    except EmptyDataError:
        log.error(f"Archivo vacío: {file_path}")
    except UnicodeDecodeError:
        log.error(f"Error al leer: {file_path}")
    except Exception as e:
        log.error(f"No se pudo cargar el archivo: {str(e)}")
    
    return None

def insert_batch(model, data, db_session, log):
    """
    Inserta registros 
    """
    if not data:
        log.warning(f"No hay datos para insertar en {model.__tablename__}.")
        return
    
    try:
        db_session.bulk_insert_mappings(model, data)
        db_session.commit()
        log.info(f" {len(data)} registros insertados en {model.__tablename__}")
        
        # Consulta para verificar los registros insertados
        registros = db_session.query(model).all()
        log.info(f"registros en {model.__tablename__}:")
        for r in registros:
            registro_dict = r.__dict__.copy()
            registro_dict.pop('_sa_instance_state', None)
            log.info(str(registro_dict))
            
    except IntegrityError:
        db_session.rollback()
        log.warning(f"id detectado en {model.__tablename__}.  actualizando...")
        
        for record in data:
            existing_record = db_session.get(model, record['id'])
            if existing_record:
                for key, value in record.items():
                    setattr(existing_record, key, value)
                log.info(f"registro con id {record['id']} actualizado en {model.__tablename__}.")
            else:
                db_session.add(model(**record))
        
        db_session.commit()
        log.info(f"Duplicados en {model.__tablename__} se actualiza para insercion")
        
        # Consulta nuevamente para verificar la actualización
        registros = db_session.query(model).all()
        log.info(f"registros en {model.__tablename__} después de actalizar:")
        for r in registros:
            registro_dict = r.__dict__.copy()
            registro_dict.pop('_sa_instance_state', None)
            log.info(str(registro_dict))
            
    except Exception as e:
        db_session.rollback()
        log.error(f"Fallo al insertar en {model.__tablename__}: {str(e)}")
        raise

def count_records(model, db_session, log):
    """
    Cuenta la cantidad de registros
    """
    try:
        count = db_session.query(model).count()
        log.info(f" Tabla {model.__tablename__} contiene {count} registros")
        return count
    except Exception as e:
        log.error(f"No se pudo contar los registros en {model.__tablename__}: {str(e)}")
        return None
