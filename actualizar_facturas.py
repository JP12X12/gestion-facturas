import sqlite3
import csv
from datetime import datetime

# Rutas
DB_PATH = r"C:\Users\Juan\Desktop\Trabajo\Proyectos\Panel unificado\bd\facturas.db"
CSV_PATH = r"C:\Users\Juan\Desktop\Trabajo\Proyectos\Panel unificado\mis_comprobantes.csv" 

def parsear_fecha(fecha_str):
    """Prueba múltiples formatos de fecha para evitar que AFIP nos rompa el código."""
    fecha_str = fecha_str.strip().split(" ")[0] # Por si viene con la hora
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(fecha_str, fmt)
        except ValueError:
            pass
    return None

def obtener_ultima_fecha(cursor):
    """Busca la fecha de emisión más reciente en la base de datos."""
    cursor.execute("SELECT fecha_emision FROM facturas WHERE fecha_emision IS NOT NULL AND fecha_emision != ''")
    fechas_crudas = cursor.fetchall()
    
    if not fechas_crudas:
        return None 
        
    fechas_validas = []
    for (f,) in fechas_crudas:
        fecha_obj = parsear_fecha(f)
        if fecha_obj:
            fechas_validas.append(fecha_obj)
            
    if not fechas_validas:
        return None
        
    return max(fechas_validas)

def armar_concatenado(cuit, tipo_comp, pv, numero):
    """Arma la clave única concatenada con tu lógica."""
    tipo_str = str(tipo_comp).strip()
    
    # Lógica de reemplazo
    if tipo_str == "6":
        tipo_str = "3"
    elif tipo_str == "1":
        tipo_str = "5"
        
    # Limpiamos espacios y rellenamos con ceros por si acaso
    pv_str = str(pv).strip().zfill(5)
    num_str = str(numero).strip().zfill(8)
    cuit_str = str(cuit).strip()
    
    return f"{cuit_str}{tipo_str}{pv_str}{num_str}"

def actualizar():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Buscar la frontera (la última fecha)
    ultima_fecha = obtener_ultima_fecha(cursor)
    
    # 2. Cargar concatenados que ya existen en la DB de esa última fecha
    concatenados_existentes = set()
    if ultima_fecha:
        # Buscamos en ambos formatos por las dudas de cómo esté guardado en DB
        f_dd_mm = ultima_fecha.strftime("%d/%m/%Y")
        f_yyyy_mm = ultima_fecha.strftime("%Y-%m-%d")
        
        cursor.execute("SELECT concatenado FROM facturas WHERE fecha_emision = ? OR fecha_emision = ?", (f_dd_mm, f_yyyy_mm))
        concatenados_existentes = {row[0] for row in cursor.fetchall()}
        print(f"📅 Última fecha en DB: {f_dd_mm}.")
        print(f"🔍 Detectadas {len(concatenados_existentes)} facturas de ese día para vigilar duplicados.")
    else:
        print("⚠️ Base de datos vacía o sin fechas. Se procesará todo el CSV.")
    
    nuevas_facturas = []
    
    # 3. Leer el CSV
    try:
        with open(CSV_PATH, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f, delimiter=';')
            
            for row in reader:
                fecha_str_csv = row.get('Fecha de Emisión', '').strip()
                if not fecha_str_csv:
                    continue
                    
                fecha_csv = parsear_fecha(fecha_str_csv)
                if not fecha_csv:
                    continue 
                    
                # Si es más vieja que la última fecha, la ignoramos directo
                if ultima_fecha and fecha_csv < ultima_fecha:
                    continue
                    
                cuit = row.get('Nro. Doc. Emisor', '')
                tipo = row.get('Tipo de Comprobante', '')
                pv = row.get('Punto de Venta', '')
                num = row.get('Número Desde', '')
                
                concatenado = armar_concatenado(cuit, tipo, pv, num)
                
                # Chequeamos si es un duplicado
                if (fecha_csv == ultima_fecha and concatenado in concatenados_existentes) or \
                   (concatenado in concatenados_existentes):
                    continue
                    
                # Parsear importes
                imp_total_str = row.get('Imp. Total', '0').replace(',', '.')
                try:
                    imp_total = float(imp_total_str)
                except ValueError:
                    imp_total = 0.0
                    
                letra = "A" if tipo == "1" else "B" if tipo == "6" else "C" if tipo == "11" else "X"
                factura_amigable = f"{letra} {str(pv).zfill(5)}-{str(num).zfill(8)}"
                
                # Guardamos siempre la fecha prolija en la DB
                fecha_db_str = fecha_csv.strftime("%d/%m/%Y")
                
                nuevas_facturas.append((
                    concatenado,
                    fecha_db_str,
                    tipo,
                    int(pv) if pv.isdigit() else 0,
                    int(num) if num.isdigit() else 0,
                    row.get('Cód. Autorización', ''),
                    cuit,
                    row.get('Denominación Emisor', ''),
                    imp_total,
                    imp_total, 
                    None,      
                    factura_amigable,
                    fecha_csv.year, 
                    None,      
                    'Generada' 
                ))
                
                concatenados_existentes.add(concatenado)

    except FileNotFoundError:
        print(f"❌ No se encontró el archivo CSV en: {CSV_PATH}")
        return

    # 4. Volcar todo a la base de datos
    if nuevas_facturas:
        cursor.executemany('''
            INSERT INTO facturas (
                concatenado, fecha_emision, tipo_comprobante, punto_venta, numero_desde,
                cod_autorizacion, nro_doc_emisor, denominacion_emisor, imp_total, importe,
                se_pago, factura, anio, correo, Estado
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', nuevas_facturas)
        conn.commit()
        print(f"✅ ¡Éxito! Se importaron {len(nuevas_facturas)} facturas nuevas.")
    else:
        print("🤷‍♂️ El sistema está al día. No se encontraron facturas nuevas en el CSV.")
        
    conn.close()

if __name__ == "__main__":
    print("===========================================")
    print(" 🚀 ACTUALIZADOR DE FACTURAS AFIP")
    print("===========================================\n")
    actualizar()
    print("\nPresioná Enter para salir...")
    input()