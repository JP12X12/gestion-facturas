import sqlite3
import os

def extraer_esquema_db():
    # Obtiene la ruta de la carpeta actual donde está el script
    ruta_actual = os.getcwd()
    archivo_salida = "estructura_columnas.txt"
    
    # Filtramos solo los archivos que terminan en .db
    archivos_db = [f for f in os.listdir(ruta_actual) if f.endswith('.db')]
    
    if not archivos_db:
        print("No se encontraron archivos .db en esta carpeta.")
        return

    with open(archivo_salida, "w", encoding="utf-8") as txt:
        for db in archivos_db:
            linea_db = f"--- BASE DE DATOS: {db} ---"
            print(linea_db)
            txt.write(linea_db + "\n")
            
            try:
                conn = sqlite3.connect(db)
                cursor = conn.cursor()
                
                # Obtenemos los nombres de todas las tablas
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tablas = cursor.fetchall()
                
                for tabla in tablas:
                    nombre_tabla = tabla[0]
                    txt.write(f"\n  Tabla: {nombre_tabla}\n")
                    print(f"  Tabla: {nombre_tabla}")
                    
                    # Obtenemos la info de las columnas de la tabla
                    cursor.execute(f"PRAGMA table_info('{nombre_tabla}');")
                    columnas = cursor.fetchall()
                    
                    # El nombre de la columna es el segundo elemento de la tupla (index 1)
                    nombres_columnas = [col[1] for col in columnas]
                    listado_cols = "    Columnas: " + ", ".join(nombres_columnas)
                    
                    print(listado_cols)
                    txt.write(listado_cols + "\n")
                
                conn.close()
                txt.write("\n" + "="*30 + "\n\n")
                print("-" * 30)
                
            except Exception as e:
                error_msg = f"Error procesando {db}: {e}"
                print(error_msg)
                txt.write(error_msg + "\n")

    print(f"\nProceso finalizado. Se generó el archivo: {archivo_salida}")

if __name__ == "__main__":
    extraer_esquema_db()