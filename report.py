import pandas as pd
import sqlite3

def report_quarterly_hired(conn):
    """
    Muestra la cantidad de empleados contratados en 2021,
    por departamento y trabajo, divididos en Q1, Q2, Q3, Q4.
    """

    # 1) Leer las tablas desde la base de datos usando pandas.read_sql_query
    df_departments = pd.read_sql_query("SELECT id, department FROM departments", conn)
    df_jobs = pd.read_sql_query("SELECT id, job FROM jobs", conn)
    df_hired = pd.read_sql_query("""
        SELECT id, name, hire_datetime, department_id, job_id
        FROM hired_employees
    """, conn)

    # 2) Convertir la columna hire_datetime a tipo fecha
    df_hired['hire_datetime'] = pd.to_datetime(df_hired['hire_datetime'], errors='coerce')

    # 3) Unir los dataframes para obtener nombres de department y job
    df_merged = df_hired.merge(df_departments, left_on='department_id', right_on='id', suffixes=('', '_dept'))
    df_merged = df_merged.merge(df_jobs, left_on='job_id', right_on='id', suffixes=('', '_job'))

    # 4) Filtrar contrataciones de 2021
    df_2021 = df_merged[df_merged['hire_datetime'].dt.year == 2021].copy()

    # 5) Obtener el trimestre con dt.quarter (valores 1..4)
    df_2021['quarter'] = df_2021['hire_datetime'].dt.quarter

    # 6) Agrupar por (department, job, quarter) y contar
    group = df_2021.groupby(['department', 'job', 'quarter']).size().reset_index(name='count')

    # 7) Pasar a formato "wide" para tener columnas Q1..Q4
    result = group.pivot(index=['department','job'], columns='quarter', values='count').fillna(0).astype(int)

    # 8) Renombrar columnas (1..4 → Q1..Q4)
    col_map = {1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}
    result.rename(columns=col_map, inplace=True)

    # 9) Convertir el índice a columnas y ordenar
    result.reset_index(inplace=True)
    result.sort_values(by=['department','job'], inplace=True)

    # 10) Mostrar
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

    # 1) Leer las tablas
    df_departments = pd.read_sql_query("SELECT id, department FROM departments", conn)
    df_jobs = pd.read_sql_query("SELECT id, job FROM jobs", conn)
    df_hired = pd.read_sql_query("""
        SELECT id, name, hire_datetime, department_id, job_id
        FROM hired_employees
    """, conn)

    # 2) Convertir la fecha
    df_hired['hire_datetime'] = pd.to_datetime(df_hired['hire_datetime'], errors='coerce')

    # 3) Unir con departments y jobs
    df_merged = df_hired.merge(df_departments, left_on='department_id', right_on='id', suffixes=('', '_dept'))
    df_merged = df_merged.merge(df_jobs, left_on='job_id', right_on='id', suffixes=('', '_job'))

    # 4) Filtrar año 2021
    df_2021 = df_merged[df_merged['hire_datetime'].dt.year == 2021].copy()

    # 5) Agrupar por (department_id, department) y contar
    group = df_2021.groupby(['department_id','department']).size().reset_index(name='hired')

    # 6) Calcular promedio
    avg_hires = group['hired'].mean()

    # 7) Filtrar los que superan el promedio
    above = group[group['hired'] > avg_hires].copy()

    # 8) Ordenar de mayor a menor
    above.sort_values(by='hired', ascending=False, inplace=True)

    # 9) Mostrar
    print("\n=== Departments Above Average in 2021 ===")
    print(f"{'ID':<5} {'Department':<25} {'Hired':<5}")
    for _, row in above.iterrows():
        print(f"{row['department_id']:<5} {row['department']:<25} {row['hired']:<5}")


def main():
    # Ajusta la ruta de tu base de datos
    conn = sqlite3.connect('migracion.db')

    # Ejecutar reporte 1
    report_quarterly_hired(conn)

    # Ejecutar reporte 2
    report_departments_above_average(conn)

    conn.close()


if __name__ == "__main__":
    main()
