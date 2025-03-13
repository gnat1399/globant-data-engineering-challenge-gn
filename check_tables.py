import sqlite3

def check_tables(db_path='migracion.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    conn.close()
    return tables

if __name__ == "__main__":
    tablas = check_tables()
    print("Tablas encontradas en la base de datos:")
    for t in tablas:
        print(t[0])
