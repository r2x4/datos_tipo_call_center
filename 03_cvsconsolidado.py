import pandas as pd
import mysql.connector

# 1. Leer el archivo CSV y eliminar espacios en los nombres de columnas
df = pd.read_csv(
    r'C:\Users\rrs23\OneDrive\Documentos\OneDrive\Documentos\Datos\BD_Consolidado.csv',
    encoding='ISO-8859-1'
)
df.columns = df.columns.str.strip()  # Eliminar espacios en nombres de columnas

# 2. Convertir la fecha al formato compatible con MySQL (YYYY-MM-DD)
df['fecha'] = pd.to_datetime(
    df['fecha'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

# 3. Convertir valores NaN a None
df = df.where(pd.notna(df), None)

# 4. Conectar con MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Dragon2307*",
    database="prueba"
)
cursor = conn.cursor()

# 5. Preparar la consulta SQL
INSERT_QUERY = """
    INSERT INTO consolidado (
        id_Registro, fecha, id_Cliente, idPrograma, nombre_servicio, id_agente, agente, 
        tiempo_logueo, tiempo_disponible, tiempo_no_listo, veces_no_listo, tiempo_acw, veces_acw, 
        tiempo_in, llamadas_in, tiempo_out, llamadas_out, tiempo_internas, veces_internas, 
        tiempo_consulta, veces_consulta, hold_in, hold_out, hold_internas, hold_consulta, 
        tiempo_ringing, tiempo_dialing, Pais
    ) 
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
"""

# 6. Convertir los datos a lista de tuplas
data_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]

# 7. Verificar coincidencia de datos
print("Número de columnas en SQL:", INSERT_QUERY.count("%s"))
print("Número de columnas en CSV:", len(df.columns))

# 8. Insertar datos en la base de datos
cursor.executemany(INSERT_QUERY, data_to_insert)
conn.commit()

# 9. Cerrar la conexión
cursor.close()
conn.close()

print("Los datos han sido insertados exitosamente.")
