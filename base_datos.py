import sqlite3
import config

def setup_dbs():
    # ordenes_pago.db
    conn = sqlite3.connect(config.DB_OP)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ordenes_pago (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha               TEXT,
            nro_op              TEXT UNIQUE,
            cuit                TEXT,
            nombre_prestador    TEXT,
            monto_transferido   REAL DEFAULT 0,
            retencion_ganancias REAL DEFAULT 0,
            retencion_iibb      REAL DEFAULT 0,
            estado              TEXT DEFAULT 'pendiente'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_facturas (
            id_op       INTEGER NOT NULL,
            id_factura  INTEGER NOT NULL,
            monto_aplicado REAL,
            PRIMARY KEY (id_op, id_factura)
        )
    """)
    conn.commit()
    conn.close()
    print("✓ ordenes_pago.db lista.")

    # facturas.db
    conn = sqlite3.connect(config.DB_FACT)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS facturas (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            concatenado         TEXT UNIQUE,
            fecha_emision       TEXT,
            tipo_comprobante    INTEGER,
            punto_venta         INTEGER,
            numero_desde        INTEGER,
            cod_autorizacion    TEXT,
            nro_doc_emisor      TEXT,
            denominacion_emisor TEXT,
            imp_total           REAL,
            importe             REAL,
            se_pago             TEXT,
            factura             TEXT,
            correo              TEXT,
            anocorreo           TEXT
        )
    """)
    # (Tus otras tablas de facturas.db)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ordenes_pago (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_op   TEXT UNIQUE,
            fecha       TEXT,
            cuit        TEXT,
            beneficiario TEXT,
            monto       REAL DEFAULT 0,
            observaciones TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS op_facturas (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            op_id       INTEGER,
            factura_id  INTEGER,
            monto_aplicado REAL,
            UNIQUE(op_id, factura_id)
        )
    """)
    conn.commit()
    conn.close()
    print("✓ facturas.db lista.")

    # transferencias.db
    conn = sqlite3.connect(config.DB_TRANS)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transferencias (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            numero           TEXT,
            numero_red       TEXT,
            fecha_solicitud  TEXT,
            comunidad        TEXT,
            nro_orden_pago   TEXT,
            nro_pago         TEXT,
            tipo_transferencia TEXT,
            cuenta_debito    TEXT,
            cuenta_credito   TEXT,
            cc_banco         TEXT,
            cc_cbu           TEXT,
            cc_cuit          TEXT,
            cc_nombre        TEXT,
            moneda           TEXT,
            importe          REAL,
            estado           TEXT
        )
    """)
    conn.commit()
    conn.close()
    print("✓ transferencias.db lista.")

def parse_importe(val):
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return 0.0
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except Exception:
        return 0.0

def sync_transferencias_to_op():
    """
    Escanea transferencias.db.
    Por cada transferencia con nro_orden_pago no vacío que NO exista
    en ordenes_pago.db, la crea automáticamente.
    """
    try:
        t_conn = sqlite3.connect(config.DB_TRANS)
        t_conn.row_factory = sqlite3.Row
        op_conn = sqlite3.connect(config.DB_OP)

        rows = t_conn.execute("""
            SELECT id, fecha_solicitud, nro_orden_pago, cc_cuit, cc_nombre, importe
            FROM transferencias
            WHERE nro_orden_pago IS NOT NULL AND trim(nro_orden_pago) != ''
        """).fetchall()

        creadas = 0
        for r in rows:
            nro_op = str(r["nro_orden_pago"]).strip()
            exists = op_conn.execute(
                "SELECT 1 FROM ordenes_pago WHERE nro_op=?", (nro_op,)
            ).fetchone()
            if not exists:
                op_conn.execute("""
                    INSERT OR IGNORE INTO ordenes_pago
                        (nro_op, fecha, cuit, nombre_prestador, monto_transferido, estado)
                    VALUES (?, ?, ?, ?, ?, 'generada')
                """, (
                    nro_op,
                    r["fecha_solicitud"] or "",
                    r["cc_cuit"] or "",
                    r["cc_nombre"] or "",
                    parse_importe(r["importe"]),
                ))
                creadas += 1

        op_conn.commit()
        t_conn.close()
        op_conn.close()
        if creadas:
            print(f"  [sync] {creadas} OPs nuevas generadas desde transferencias.")
    except Exception as e:
        print(f"  [sync] Error: {e}")