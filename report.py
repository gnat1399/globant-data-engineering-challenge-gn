import pandas as pd
import sqlite3

def report_quarterly_hired(conn):
    """
    Muestra la cantidad de empleados contratados en 2021,
    por departamento y trabajo, divididos en Q1, Q2, Q3, Q4.
    """

    # Lectura de las tablas desde la base de datos
    df_departments = pd.read_sql_query("SELECT id, department FROM departments", conn)
    df_jobs = pd.read_sql_query("SELECT id, job FROM jobs", conn)
    df_hired = pd.read_sql_query("""
        SELECT id, name, hire_datetime, department_id, job_id
        FROM hired_employees
    """, conn)

    # Se convierte hire_datetime a tipo fecha
    df_hired['hire_datetime'] = pd.to_datetime(df_hired['hire_datetime'], errors='coerce')

    # Unicon de los dataframes para obtener nombres de department y job
    df_merged = df_hired.merge(df_departments, left_on='department_id', right_on='id', suffixes=('', '_dept'))
    df_merged = df_merged.merge(df_jobs, left_on='job_id', right_on='id', suffixes=('', '_job'))

    # Filtro contrataciones de 2021
    df_2021 = df_merged[df_merged['hire_datetime'].dt.year == 2021].copy()

    # Obtencion del trimestre
    df_2021['quarter'] = df_2021['hire_datetime'].dt.quarter

    # Agrupamiento y conteo
    group = df_2021.groupby(['department', 'job', 'quarter']).size().reset_index(name='count')

    result = group.pivot(index=['department','job'], columns='quarter', values='count').fillna(0).astype(int)

    col_map = {1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}
    result.rename(columns=col_map, inplace=True)

    result.reset_index(inplace=True)
    result.sort_values(by=['department','job'], inplace=True)

    print("=== Quarterly Hires in 2021 (by department/job) ===")
    print(f"{'Department':<25} {'Job':<30} {'Q1':<5} {'Q2':<5} {'Q3':<5} {'Q4':<5}")
    for _, row in result.iterrows():
        print(f"{row['department']:<25} {row['job']:<30} "
              f"{row.get('Q1', 0):<5} {row.get('Q2', 0):<5} "
              f"{row.get('Q3', 0):<5} {row.get('Q4', 0):<5}")


def report_departments_above_average(conn):
    """
    Muestra (id, department, hired) de los departamentos que
    contrataron más empleados que el promedio de todos los departamentos en 2021.
    """

    # Lectura de las tablas desde la base de datos
    df_departments = pd.read_sql_query("SELECT id, department FROM departments", conn)
    df_jobs = pd.read_sql_query("SELECT id, job FROM jobs", conn)
    df_hired = pd.read_sql_query("""
        SELECT id, name, hire_datetime, department_id, job_id
        FROM hired_employees
    """, conn)

    # Se convierte hire_datetime a tipo fecha
    df_hired['hire_datetime'] = pd.to_datetime(df_hired['hire_datetime'], errors='coerce')

    # Unicon de los dataframes para obtener nombres de department y job
    df_merged = df_hired.merge(df_departments, left_on='department_id', right_on='id', suffixes=('', '_dept'))
    df_merged = df_merged.merge(df_jobs, left_on='job_id', right_on='id', suffixes=('', '_job'))

    # Filtro
    df_2021 = df_merged[df_merged['hire_datetime'].dt.year == 2021].copy()

    group = df_2021.groupby(['department_id','department']).size().reset_index(name='hired')

    avg_hires = group['hired'].mean()

    above = group[group['hired'] > avg_hires].copy()

    above.sort_values(by='hired', ascending=False, inplace=True)

    print("\n=== Departments Above Average in 2021 ===")
    print(f"{'ID':<5} {'Department':<25} {'Hired':<5}")
    for _, row in above.iterrows():
        print(f"{row['department_id']:<5} {row['department']:<25} {row['hired']:<5}")


def main():
    # Ruta de base de datos
    conn = sqlite3.connect('migration.db')

    # Ejecucion
    report_quarterly_hired(conn)
    report_departments_above_average(conn)

    conn.close()


if __name__ == "__main__":
    main()
