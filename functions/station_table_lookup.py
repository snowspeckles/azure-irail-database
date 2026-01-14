import os
import pyodbc

conn = pyodbc.connect(os.environ["SQL_CONNECTION_STRING"], autocommit=True)
cursor = conn.cursor()
cursor.execute("SELECT TOP 20 * FROM Stations ORDER BY collected_at DESC")

for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
