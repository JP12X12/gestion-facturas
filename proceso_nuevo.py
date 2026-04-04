import sqlite3
import os


def preparar_tablas():
    conn = sqlite3.connect('facturas.db')
    cursor = conn.cursor()

    # Intentamos agregar las columnas. 
    # Si ya existen, el programa saltará al 'except' sin romperse.
    columnas_a_agregar = [
        "ALTER TABLE ordenes_pago ADD COLUMN ret_ganancias REAL DEFAULT NULL",
        "ALTER TABLE ordenes_pago ADD COLUMN ret_iibb REAL DEFAULT NULL"
    ]

    for sentencia in columnas_a_agregar:
        try:
            cursor.execute(sentencia)
            print(f"Columna creada con éxito.")
        except sqlite3.OperationalError:
            print(f"La columna ya existía, saltando...")

    conn.commit()
    conn.close()


 
 
# Ejecutar la preparación
preparar_tablas()

def inicializar_y_procesar():
    # Rutas de tus archivos (ajustar si están en otra carpeta)
    path_facturas = 'facturas.db'
    path_trans = 'transferencias.db'

    # Conectar a la base de facturas
    conn = sqlite3.connect(path_facturas)
    cursor = conn.cursor()

    # 1. Asegurar que existan las columnas de retenciones
    try:
        cursor.execute("ALTER TABLE ordenes_pago ADD COLUMN ret_ganancias REAL")
        cursor.execute("ALTER TABLE ordenes_pago ADD COLUMN ret_iibb REAL")
        print("Columnas de retenciones añadidas.")
    except sqlite3.OperationalError:
        # Si ya existen, SQL tirará error, simplemente lo ignoramos
        pass

    # 2. 'Attach' de la segunda base de datos para trabajar con ambas a la vez
    # Esto permite hacer consultas que crucen tablas de distintos archivos .db
    cursor.execute(f"ATTACH DATABASE '{path_trans}' AS db_trans")

    # 3. Traer transferencias que tengan Nro de OP y que NO estén ya en ordenes_pago
    # Usamos el CUIT de la transferencia para machear con el emisor de la factura
    query_transferencias = """
        SELECT nro_orden_pago, fecha_solicitud, cc_cuit, cc_nombre, importe 
        FROM db_trans.transferencias 
        WHERE nro_orden_pago IS NOT NULL
        AND nro_orden_pago NOT IN (SELECT numero_op FROM ordenes_pago)
    """
    cursor.execute(query_transferencias)
    nuevas_ops = cursor.fetchall()

    for nro_op, fecha, cuit, nombre, monto_op in nuevas_ops:
        # Insertar la OP con retenciones vacías (NULL)
        cursor.execute("""
            INSERT INTO ordenes_pago (numero_op, fecha, cuit, beneficiario, monto, ret_ganancias, ret_iibb)
            VALUES (?, ?, ?, ?, ?, NULL, NULL)
        """, (nro_op, fecha, cuit, nombre, monto_op))
        
        op_id = cursor.lastrowid
        saldo_disponible = monto_op

        # 4. Buscar facturas pendientes del mismo proveedor
        # Nota: nro_doc_emisor en tu tabla facturas debe coincidir con cc_cuit
        cursor.execute("""
            SELECT id, imp_total FROM facturas 
            WHERE nro_doc_emisor = ? AND (se_pago = 'No' OR se_pago IS NULL)
            ORDER BY fecha_emision ASC
        """, (cuit,))
        
        facturas = cursor.fetchall()

        for f_id, f_total in facturas:
            if saldo_disponible <= 0:
                break
            
            pago_parcial = min(saldo_disponible, f_total)

            # Registrar vinculación
            cursor.execute("""
                INSERT INTO op_facturas (op_id, factura_id, monto_aplicado)
                VALUES (?, ?, ?)
            """, (op_id, f_id, pago_parcial))

            # Si la factura se cubrió, marcar como pagada
            if pago_parcial >= f_total:
                cursor.execute("UPDATE facturas SET se_pago = 'Si' WHERE id = ?", (f_id,))
            
            saldo_disponible -= pago_parcial

    conn.commit()
    conn.close()
    print(f"Proceso finalizado. Se procesaron {len(nuevas_ops)} nuevas órdenes de pago.")

if __name__ == "__main__":
    inicializar_y_procesar()