#!/usr/bin/env python3
"""
Servidor unificado: Facturas + Transferencias + Órdenes de Pago.
Concepto: "La Transferencia manda" — las OPs se generan automáticamente
desde transferencias.db cuando tienen nro_orden_pago.

Rutas HTML:
  /               → panel.html
  /facturas       → index_facturas.html
  /trans          → index_transferencias.html

APIs:
  /facturas/api/...   → Facturas (data, stats, preview, confirm, csv, etc.)
  /trans/api/...      → Transferencias (data, stats, preview, confirm, csv)
  /ops/api/...        → Cuenta corriente (unión facturas + OPs por CUIT)
"""
import auth # Tu nuevo archivo
import urllib.parse
import http.server
import socketserver
import sqlite3
import json
import os
import re
import io
import csv
import threading
import time
import random
import urllib.parse
import socket

HOST     = "0.0.0.0"
PORT     = 5000
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Definimos las nuevas subcarpetas
HTML_DIR = os.path.join(BASE_DIR, "htmls")
BD_DIR   = os.path.join(BASE_DIR, "bd")

# ── Rutas a los HTML ──
HTML_PANEL = os.path.join(HTML_DIR, "panel.html")
HTML_FACT  = os.path.join(HTML_DIR, "index_facturas.html")
HTML_TRANS = os.path.join(HTML_DIR, "index_transferencias.html")
HTML_OPS   = os.path.join(HTML_DIR, "index_op.html")
HTML_LOGIN = os.path.join(HTML_DIR, "login.html")
HTML_ADMIN = os.path.join(HTML_DIR, "admin_sesiones.html")

# ── Rutas a las Bases de Datos ──
DB_FACT  = os.path.join(BD_DIR, "facturas.db")
DB_TRANS = os.path.join(BD_DIR, "transferencias.db")
DB_OP    = os.path.join(BD_DIR, "ordenes_pago.db")

# ══════════════════════════════════════════════════════════════════════
# INIT / SETUP
# ══════════════════════════════════════════════════════════════════════

def setup_dbs():
    # ordenes_pago.db
    conn = sqlite3.connect(DB_OP)
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
    conn = sqlite3.connect(DB_FACT)
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
    conn = sqlite3.connect(DB_TRANS)
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


# ══════════════════════════════════════════════════════════════════════
# SYNC: Transferencias → OPs (el corazón del sistema)
# ══════════════════════════════════════════════════════════════════════

def sync_transferencias_to_op():
    """
    Escanea transferencias.db.
    Por cada transferencia con nro_orden_pago no vacío que NO exista
    en ordenes_pago.db, la crea automáticamente.
    """
    try:
        t_conn = sqlite3.connect(DB_TRANS)
        t_conn.row_factory = sqlite3.Row
        op_conn = sqlite3.connect(DB_OP)

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


# ══════════════════════════════════════════════════════════════════════
# JOBS (background processing)
# ══════════════════════════════════════════════════════════════════════

_jobs      = {}
_jobs_lock = threading.Lock()
_pending      = {}
_pending_lock = threading.Lock()

def _make_id():
    return f"{int(time.time())}{random.randint(10000, 99999)}"


# ══════════════════════════════════════════════════════════════════════
# HELPERS GENERALES
# ══════════════════════════════════════════════════════════════════════

def parse_importe(val):
    """Convierte importes en formato argentino '1.590.354,00' o '1590354.00' a float."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return 0.0
    # Formato argentino: puntos como miles, coma como decimal
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except Exception:
        return 0.0

def normalizar_fecha(val):
    if val is None:
        return ""
    s = str(val).strip()
    if " " in s:
        s = s.split(" ")[0]
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        p = s.split("-")
        return f"{p[2]}/{p[1]}/{p[0]}"
    return s

def normalizar_sepago(val):
    if val is None or str(val).strip().lower() in ("", "nan", "none"):
        return ""
    return re.sub(r"\D", "", str(val))

def tipo_letra(tipo):
    m = {1: "A", 6: "B", 11: "C"}
    try:
        return m.get(int(float(tipo)), "NC")
    except Exception:
        return "NC"

def build_factura(tipo, punto_venta, numero_desde):
    letra = tipo_letra(tipo)
    try:
        pv = str(int(float(punto_venta))).zfill(5) if punto_venta not in (None, "", "nan") else "00000"
    except Exception:
        pv = "00000"
    try:
        nd = str(int(float(numero_desde))).zfill(8) if numero_desde not in (None, "", "nan") else "00000000"
    except Exception:
        nd = "00000000"
    return f"{letra} {pv}-{nd}"


# ══════════════════════════════════════════════════════════════════════
# FACTURAS — lógica de datos
# ══════════════════════════════════════════════════════════════════════

def facturas_stats():
    try:
        conn = sqlite3.connect(DB_FACT)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM facturas")
        total = c.fetchone()[0] or 0
        c.execute("SELECT COUNT(*) FROM facturas WHERE se_pago IS NOT NULL AND se_pago != ''")
        pagadas = c.fetchone()[0] or 0
        c.execute("SELECT COUNT(*) FROM facturas WHERE se_pago IS NULL OR se_pago = ''")
        impagas = c.fetchone()[0] or 0
        c.execute("SELECT COALESCE(SUM(importe),0) FROM facturas")
        total_importe = c.fetchone()[0] or 0
        conn.close()
        return {"total": total, "pagadas": pagadas, "impagas": impagas, "total_importe": total_importe}
    except Exception:
        return {"total": 0, "pagadas": 0, "impagas": 0, "total_importe": 0}

def _fact_where(filters):
    where  = "WHERE 1=1"
    params = []
    if not filters:
        return where, params
    if filters.get("fecha_desde"):
        d = filters["fecha_desde"]
        p = d.split("/")
        ymd = (p[2]+p[1]+p[0]) if len(p)==3 else d
        where += " AND substr(fecha_emision,7,4)||substr(fecha_emision,4,2)||substr(fecha_emision,1,2) >= ?"
        params.append(ymd)
    if filters.get("fecha_hasta"):
        d = filters["fecha_hasta"]
        p = d.split("/")
        ymd = (p[2]+p[1]+p[0]) if len(p)==3 else d
        where += " AND substr(fecha_emision,7,4)||substr(fecha_emision,4,2)||substr(fecha_emision,1,2) <= ?"
        params.append(ymd)
    if filters.get("factura"):
        where += " AND factura LIKE ?"
        params.append(f"%{filters['factura']}%")
    if filters.get("emisor"):
        where += " AND (denominacion_emisor LIKE ? OR nro_doc_emisor LIKE ?)"
        v = f"%{filters['emisor']}%"
        params.extend([v, v])
    if filters.get("tipo"):
        where += " AND tipo_comprobante = ?"
        params.append(filters["tipo"])
    if filters.get("solo_pagados") == "1":
        where += " AND se_pago IS NOT NULL AND se_pago != ''"
    if filters.get("solo_impagos") == "1":
        where += " AND (se_pago IS NULL OR se_pago = '')"
    return where, params

def _fact_sort(sort):
    fe = "substr(fecha_emision,7,4)||substr(fecha_emision,4,2)||substr(fecha_emision,1,2)"
    m = {
        "fecha_asc":    f"{fe} ASC,  id ASC",
        "fecha_desc":   f"{fe} DESC, id DESC",
        "importe_asc":  "importe ASC",
        "importe_desc": "importe DESC",
    }
    return m.get(sort, f"{fe} DESC, id DESC")

def facturas_query(params_qs):
    page      = max(1, int(params_qs.get("page",      ["1"])[0]   or 1))
    page_size = max(10, min(int(params_qs.get("limit", ["20"])[0] or 20), 1000))
    sort      = params_qs.get("sort", ["fecha_desc"])[0]
    filters   = {k: v[0] for k, v in params_qs.items() if v and v[0]}

    where, args = _fact_where(filters)
    order = _fact_sort(sort)

    conn = sqlite3.connect(DB_FACT)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute(f"SELECT COUNT(*) FROM facturas {where}", args)
    total = c.fetchone()[0] or 0

    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    offset = (page - 1) * page_size

    c.execute(f"SELECT * FROM facturas {where} ORDER BY {order} LIMIT ? OFFSET ?",
              args + [page_size, offset])
    rows = [dict(r) for r in c.fetchall()]

    # Agregar OPs vinculadas a cada factura
    if rows:
        ids = [r["id"] for r in rows]
        ph  = ",".join("?" * len(ids))
        c.execute(f"""
            SELECT opf.factura_id, op.id as op_id, op.numero_op
            FROM op_facturas opf
            JOIN ordenes_pago op ON op.id = opf.op_id
            WHERE opf.factura_id IN ({ph})
        """, ids)
        ops_map = {}
        for r in c.fetchall():
            ops_map.setdefault(r[0], []).append({"op_id": r[1], "numero_op": r[2]})
        for row in rows:
            row["ops"] = ops_map.get(row["id"], [])

    conn.close()
    return {"rows": rows, "total": total, "page": page,
            "total_pages": total_pages, "page_size": page_size}

def facturas_csv(params_qs):
    filters = {k: v[0] for k, v in params_qs.items() if v and v[0]}
    sort    = params_qs.get("sort", ["fecha_desc"])[0]
    where, args = _fact_where(filters)
    order = _fact_sort(sort)
    conn = sqlite3.connect(DB_FACT)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"SELECT * FROM facturas {where} ORDER BY {order}", args).fetchall()
    rows = [dict(r) for r in rows]
    conn.close()

    buf = io.StringIO()
    buf.write("\ufeff")
    cols = [
        ("factura","Factura"),("fecha_emision","Fecha Emisión"),
        ("nro_doc_emisor","CUIT Emisor"),("denominacion_emisor","Denominación Emisor"),
        ("importe","Importe"),("imp_total","Imp. Total"),
        ("anocorreo","Correo"),("se_pago","Se Pagó"),("concatenado","Concatenado"),
        ("cod_autorizacion","Cód. Autorización"),
    ]
    w = csv.writer(buf, delimiter=";")
    w.writerow([h for _,h in cols])
    for r in rows:
        w.writerow([r.get(k,"") for k,_ in cols])
    return buf.getvalue().encode("utf-8-sig")


# ══════════════════════════════════════════════════════════════════════
# FACTURAS — importación Excel (background job)
# ══════════════════════════════════════════════════════════════════════

def _process_excel_facturas(job_id, file_bytes, check_dupes=True):
    def upd(step, pct):
        with _jobs_lock:
            _jobs[job_id].update({"step": step, "pct": pct})

    try:
        upd("Leyendo archivo Excel…", 5)
        import pandas as pd

        buf = io.BytesIO(file_bytes)
        df  = None
        for engine in ("calamine", "openpyxl"):
            try:
                buf.seek(0)
                df = pd.read_excel(buf, dtype=str, engine=engine)
                break
            except Exception:
                pass
        if df is None:
            raise Exception("No se pudo leer el Excel.")

        upd("Normalizando columnas…", 25)
        df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]
        known = {
            "concatenado":"concatenado",
            "fecha de emision":"fecha_emision","fecha de emisión":"fecha_emision",
            "tipo de comprobante":"tipo_comprobante",
            "punto de venta":"punto_venta",
            "número desde":"numero_desde","numero desde":"numero_desde",
            "cod. autorizacion":"cod_autorizacion","cód. autorización":"cod_autorizacion",
            "nro. doc. emisor":"nro_doc_emisor",
            "denominacion emisor":"denominacion_emisor","denominación emisor":"denominacion_emisor",
            "imp. total":"imp_total","importe":"importe",
            "se pago":"se_pago","se pagó":"se_pago",
            "correo":"correo",
            "anocorreo":"anocorreo",
        }
        col_map = {}
        for orig in df.columns:
            key = orig.lower().strip()
            if key in known:
                col_map[known[key]] = orig

        fallback = [
            "concatenado","fecha_emision","tipo_comprobante","punto_venta",
            "numero_desde",None,"cod_autorizacion",None,"nro_doc_emisor",
            "denominacion_emisor",None,None,None,None,None,None,None,None,
            None,None,None,None,None,None,None,None,None,None,None,None,
            "imp_total","importe","se_pago","correo","anocorreo"
        ]
        for i, fld in enumerate(fallback):
            if fld and fld not in col_map and i < len(df.columns):
                col_map[fld] = df.columns[i]

        def gv(row, field, default=""):
            col = col_map.get(field)
            if col and col in row.index:
                v = str(row[col]).strip()
                return default if v.lower() in ("nan","none","") else v
            return default

        upd("Procesando filas…", 40)
        records = []
        for _, row in df.iterrows():
            concat = gv(row, "concatenado")
            if not concat:
                continue
            tipo  = gv(row, "tipo_comprobante")
            pv    = gv(row, "punto_venta")
            nd    = gv(row, "numero_desde")
            try: imp_total = float(gv(row, "imp_total", "0") or 0)
            except: imp_total = 0.0
            try: importe = float(gv(row, "importe", "0") or 0)
            except: importe = 0.0
            records.append({
                "concatenado":         concat,
                "fecha_emision":       normalizar_fecha(gv(row, "fecha_emision")),
                "tipo_comprobante":    tipo,
                "punto_venta":         pv,
                "numero_desde":        nd,
                "cod_autorizacion":    gv(row, "cod_autorizacion"),
                "nro_doc_emisor":      gv(row, "nro_doc_emisor"),
                "denominacion_emisor": gv(row, "denominacion_emisor"),
                "imp_total":           imp_total,
                "importe":             importe,
                "se_pago":             normalizar_sepago(gv(row, "se_pago")),
                "factura":             build_factura(tipo, pv, nd),
                "correo":              gv(row, "correo"),
                "anocorreo":           gv(row, "anocorreo"),
            })

        if not records:
            raise Exception("No se encontraron registros válidos.")

        new_recs = []
        dup_recs = []

        if check_dupes:
            upd(f"Buscando duplicados… ({len(records):,} registros)", 70)
            conn = sqlite3.connect(DB_FACT)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("SELECT concatenado FROM facturas")
            existing = {r[0] for r in c.fetchall()}

            dup_keys = {r["concatenado"] for r in records} & existing
            existing_map = {}
            if dup_keys:
                ph = ",".join("?"*len(dup_keys))
                c.execute(f"SELECT * FROM facturas WHERE concatenado IN ({ph})", list(dup_keys))
                existing_map = {r["concatenado"]: dict(r) for r in c.fetchall()}
            conn.close()

            for r in records:
                if r["concatenado"] in existing:
                    dup_recs.append({"incoming": r, "existing": existing_map.get(r["concatenado"], {})})
                else:
                    new_recs.append(r)
        else:
            new_recs = records

        token = _make_id()
        with _pending_lock:
            _pending[token] = {"new": new_recs, "dupes": dup_recs, "type": "facturas"}

        upd("Listo", 100)
        with _jobs_lock:
            _jobs[job_id].update({
                "status": "done",
                "result": {
                    "nuevos":    len(new_recs),
                    "duplicados": len(dup_recs),
                    "total":     len(records),
                    "token":     token,
                    "dupes_preview": [
                        {
                            "factura":     d["existing"].get("factura",""),
                            "emisor":      d["existing"].get("denominacion_emisor",""),
                            "importe_old": d["existing"].get("importe",""),
                            "importe_new": d["incoming"]["importe"],
                            "sepago_old":  d["existing"].get("se_pago",""),
                            "sepago_new":  d["incoming"]["se_pago"],
                        }
                        for d in dup_recs[:50]
                    ]
                }
            })
    except Exception as e:
        import traceback; traceback.print_exc()
        with _jobs_lock:
            _jobs[job_id].update({
                "status": "error",
                "error": str(e)
            })

def confirm_facturas(token, update_dupes=False):
    with _pending_lock:
        data = _pending.pop(token, None)
    if not data:
        return 0, 0, 0, "Token inválido."

    new_recs = data.get("new", [])
    dup_recs = data.get("dupes", [])

    conn = sqlite3.connect(DB_FACT)
    ins, upd, skp = 0, 0, 0

    # Insertar nuevos
    for r in new_recs:
        try:
            conn.execute("""
                INSERT INTO facturas
                (concatenado, fecha_emision, tipo_comprobante, punto_venta, numero_desde,
                 cod_autorizacion, nro_doc_emisor, denominacion_emisor, imp_total, importe,
                 se_pago, factura, correo, anocorreo)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r["concatenado"], r["fecha_emision"], r["tipo_comprobante"],
                r["punto_venta"], r["numero_desde"], r["cod_autorizacion"],
                r["nro_doc_emisor"], r["denominacion_emisor"], r["imp_total"],
                r["importe"], r["se_pago"], r["factura"],
                r.get("correo", ""), r.get("anocorreo", "")
            ))
            ins += 1
        except Exception:
            skp += 1

    # Procesar duplicados
    if update_dupes:
        for d in dup_recs:
            concat = d["incoming"]["concatenado"]
            try:
                conn.execute("""
                    UPDATE facturas
                    SET se_pago=?, importe=?, anocorreo=?
                    WHERE concatenado=?
                """, (
                    d["incoming"]["se_pago"],
                    d["incoming"]["importe"],
                    d["incoming"].get("anocorreo", ""),
                    concat
                ))
                upd += 1
            except Exception:
                skp += 1
    else:
        skp += len(dup_recs)

    conn.commit()
    conn.close()

    msg = f"Insertados: {ins}, Actualizados: {upd}, Omitidos: {skp}"
    return ins, upd, skp, msg


# ══════════════════════════════════════════════════════════════════════
# TRANSFERENCIAS
# ══════════════════════════════════════════════════════════════════════

def trans_stats():
    try:
        conn = sqlite3.connect(DB_TRANS)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM transferencias")
        total = c.fetchone()[0] or 0
        c.execute("SELECT COALESCE(SUM(importe),0) FROM transferencias")
        total_importe = c.fetchone()[0] or 0
        c.execute("SELECT estado, COUNT(*) FROM transferencias GROUP BY estado")
        estados = {(r[0] or ""): r[1] for r in c.fetchall()}
        conn.close()
        return {"total": total, "total_importe": total_importe, "estados": estados}
    except Exception:
        return {"total": 0, "total_importe": 0, "estados": {}}

def trans_query(params_qs):
    page      = max(1, int(params_qs.get("page",      ["1"])[0]   or 1))
    page_size = max(10, min(int(params_qs.get("page_size", params_qs.get("limit", ["100"]))[0] or 100), 1000))
    sort      = params_qs.get("sort", ["fecha_desc"])[0]

    # Build WHERE from filters
    where  = "WHERE 1=1"
    args   = []

    fecha_desde = params_qs.get("fecha_desde", [""])[0]
    fecha_hasta = params_qs.get("fecha_hasta", [""])[0]
    nro_op      = params_qs.get("nro_op",  [""])[0]
    op_desde    = params_qs.get("op_desde",[""])[0]
    op_hasta    = params_qs.get("op_hasta",[""])[0]
    cuit        = params_qs.get("cuit",    [""])[0]
    debito      = params_qs.get("debito",  [""])[0]
    estado      = params_qs.get("estado",  [""])[0]

    def ymd(d):
        p = d.split("/")
        return (p[2]+p[1]+p[0]) if len(p)==3 else d

    if fecha_desde:
        where += " AND replace(substr(fecha_solicitud,1,10),'-','') >= ?"
        args.append(ymd(fecha_desde))
    if fecha_hasta:
        where += " AND replace(substr(fecha_solicitud,1,10),'-','') <= ?"
        args.append(ymd(fecha_hasta))
    if nro_op:
        where += " AND nro_orden_pago LIKE ?"
        args.append(f"%{nro_op}%")
    if op_desde:
        where += " AND CAST(nro_orden_pago AS INTEGER) >= ?"
        args.append(int(op_desde))
    if op_hasta:
        where += " AND CAST(nro_orden_pago AS INTEGER) <= ?"
        args.append(int(op_hasta))
    if cuit:
        where += " AND (cc_cuit LIKE ? OR cc_cbu LIKE ? OR cc_nombre LIKE ?)"
        v = f"%{cuit}%"
        args.extend([v, v, v])
    if debito:
        where += " AND cuenta_debito = ?"
        args.append(debito)
    if estado:
        where += " AND estado LIKE ?"
        args.append(f"%{estado}%")

    order = "fecha_solicitud DESC" if sort != "fecha_asc" else "fecha_solicitud ASC"

    conn = sqlite3.connect(DB_TRANS)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute(f"SELECT COUNT(*) FROM transferencias {where}", args)
    total = c.fetchone()[0] or 0

    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    offset = (page - 1) * page_size

    c.execute(f"SELECT * FROM transferencias {where} ORDER BY {order} LIMIT ? OFFSET ?",
              args + [page_size, offset])
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    return {"rows": rows, "total": total, "page": page,
            "total_pages": total_pages, "page_size": page_size}

def trans_csv(params_qs):
    conn = sqlite3.connect(DB_TRANS)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM transferencias ORDER BY fecha_solicitud DESC").fetchall()
    rows = [dict(r) for r in rows]
    conn.close()

    buf = io.StringIO()
    buf.write("\ufeff")
    cols = [
        ("numero","Número"),("fecha_solicitud","Fecha Solicitud"),
        ("comunidad","Comunidad"),("nro_orden_pago","Nro OP"),
        ("importe","Importe"),("estado","Estado"),
    ]
    w = csv.writer(buf, delimiter=";")
    w.writerow([h for _,h in cols])
    for r in rows:
        w.writerow([r.get(k,"") for k,_ in cols])
    return buf.getvalue().encode("utf-8-sig")

def _process_txt_trans(job_id, file_bytes):
    def upd(step, pct):
        with _jobs_lock:
            _jobs[job_id].update({"step": step, "pct": pct})

    try:
        upd("Leyendo archivo…", 10)
        text = file_bytes.decode("utf-8", errors="ignore")
        lines = text.strip().split("\n")

        upd("Procesando líneas…", 50)
        records = []
        for line in lines:
            if not line.strip():
                continue
            parts = line.split("|")
            if len(parts) >= 5:
                records.append({
                    "numero": parts[0].strip() if len(parts) > 0 else "",
                    "fecha_solicitud": parts[1].strip() if len(parts) > 1 else "",
                    "comunidad": parts[2].strip() if len(parts) > 2 else "",
                    "nro_orden_pago": parts[3].strip() if len(parts) > 3 else "",
                    "importe": parse_importe(parts[4].strip() if len(parts) > 4 else "0"),
                })

        if not records:
            raise Exception("No se encontraron registros válidos.")

        token = _make_id()
        with _pending_lock:
            _pending[token] = {"new": records, "dupes": [], "type": "trans"}

        upd("Listo", 100)
        with _jobs_lock:
            _jobs[job_id].update({
                "status": "done",
                "result": {
                    "nuevos": len(records),
                    "duplicados": 0,
                    "total": len(records),
                    "token": token,
                }
            })
    except Exception as e:
        with _jobs_lock:
            _jobs[job_id].update({
                "status": "error",
                "error": str(e)
            })

def confirm_trans(token, update_dupes=False):
    with _pending_lock:
        data = _pending.pop(token, None)
    if not data:
        return 0, 0, 0, "Token inválido."

    records = data.get("new", [])
    conn = sqlite3.connect(DB_TRANS)
    ins, upd, skp = 0, 0, 0

    for r in records:
        try:
            conn.execute("""
                INSERT INTO transferencias
                (numero, fecha_solicitud, comunidad, nro_orden_pago, importe)
                VALUES (?, ?, ?, ?, ?)
            """, (
                r["numero"], r["fecha_solicitud"], r["comunidad"],
                r["nro_orden_pago"], r["importe"]
            ))
            ins += 1
        except Exception:
            skp += 1

    conn.commit()
    conn.close()
    msg = f"Insertados: {ins}, Omitidos: {skp}"
    return ins, 0, skp, msg


# ══════════════════════════════════════════════════════════════════════
# CUENTA CORRIENTE / OPs
# ══════════════════════════════════════════════════════════════════════

def ops_stats():
    try:
        conn = sqlite3.connect(DB_OP)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM ordenes_pago")
        total = c.fetchone()[0] or 0
        c.execute("SELECT COALESCE(SUM(monto_transferido),0) FROM ordenes_pago")
        total_monto = c.fetchone()[0] or 0
        conn.close()
        return {"total": total, "total_monto": total_monto}
    except Exception:
        return {"total": 0, "total_monto": 0}

def cuenta_corriente(params_qs):
    cuit = params_qs.get("cuit", [""])[0]
    page = max(1, int(params_qs.get("page", ["1"])[0] or 1))
    page_size = 20

    conn = sqlite3.connect(DB_OP)
    conn.row_factory = sqlite3.Row

    where = "WHERE 1=1"
    if cuit:
        where += " AND cuit = ?"
        args = [cuit]
    else:
        args = []

    c = conn.cursor()
    c.execute(f"SELECT COUNT(*) FROM ordenes_pago {where}", args)
    total = c.fetchone()[0] or 0

    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(page, total_pages)
    offset = (page - 1) * page_size

    c.execute(f"SELECT * FROM ordenes_pago {where} ORDER BY fecha DESC LIMIT ? OFFSET ?",
              args + [page_size, offset])
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    return {"rows": rows, "total": total, "page": page, "total_pages": total_pages}


def cuenta_corriente_prestador(q):
    """
    Devuelve una lista unificada (filas mezcladas) de facturas y OPs,
    ordenada por fecha descendente.
    Cada item tiene tipo: 'factura' | 'op'.
    También devuelve listas separadas para el modal.
    """
    if not q:
        return {"rows": [], "facturas": [], "ops": []}

    like = f"%{q}%"

    facturas = []
    try:
        conn = sqlite3.connect(DB_FACT)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT id, fecha_emision, tipo_comprobante, punto_venta, numero_desde,
                   imp_total, importe, se_pago, denominacion_emisor, nro_doc_emisor, factura
            FROM facturas
            WHERE nro_doc_emisor LIKE ? OR denominacion_emisor LIKE ?
        """, (like, like)).fetchall()
        for r in rows:
            facturas.append({
                "tipo":          "factura",
                "id":            r["id"],
                "fecha":         r["fecha_emision"] or "",
                "fecha_sort":    (lambda d: d.split("/")[2]+d.split("/")[1]+d.split("/")[0] if d.count("/")==2 else d)(r["fecha_emision"] or ""),
                "punto_venta":   r["punto_venta"]   or "",
                "numero_desde":  r["numero_desde"]  or "",
                "factura":       r["factura"]        or "",
                "imp_total":     r["imp_total"]      or 0,
                "importe":       r["importe"]        or 0,
                "se_pago":       r["se_pago"]        or "",
                "denominacion":  r["denominacion_emisor"] or "",
                "cuit":          r["nro_doc_emisor"] or "",
            })
        conn.close()
    except Exception as e:
        print(f"  [cc_prestador] Error facturas: {e}")

    ops = []
    try:
        conn = sqlite3.connect(DB_OP)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT id, fecha, nro_op, nombre_prestador, cuit,
                   monto_transferido, retencion_ganancias, retencion_iibb, estado
            FROM ordenes_pago
            WHERE cuit LIKE ? OR nombre_prestador LIKE ?
        """, (like, like)).fetchall()
        for r in rows:
            fact_ids = [x[0] for x in conn.execute(
                "SELECT id_factura FROM op_facturas WHERE id_op=?", (r["id"],)
            ).fetchall()]
            montos_aplicados = {x[0]: x[1] for x in conn.execute(
                "SELECT id_factura, monto_aplicado FROM op_facturas WHERE id_op=?", (r["id"],)
            ).fetchall()}
            monto_aplicado_total = sum(montos_aplicados.values())
            ret_g = r["retencion_ganancias"] or 0
            ret_i = r["retencion_iibb"]      or 0
            saldo = (r["monto_transferido"] or 0) - monto_aplicado_total + ret_g + ret_i
            fecha = r["fecha"] or ""
            ops.append({
                "tipo":          "op",
                "id":            r["id"],
                "fecha":         fecha,
                "fecha_sort":    fecha.replace("-","").replace("/",""),
                "numero_op":     r["nro_op"]             or "",
                "monto":         r["monto_transferido"]  or 0,
                "ret_ganancias": ret_g,
                "ret_iibb":      ret_i,
                "estado":        r["estado"]             or "",
                "nombre":        r["nombre_prestador"]   or "",
                "cuit":          r["cuit"]               or "",
                "fact_ids":      fact_ids,
                "montos_aplicados": montos_aplicados,
                "monto_aplicado_total": monto_aplicado_total,
                "saldo":         saldo,
            })
        conn.close()
    except Exception as e:
        print(f"  [cc_prestador] Error OPs: {e}")

    # Mezclar y ordenar por fecha desc
    unified = sorted(facturas + ops, key=lambda x: x["fecha_sort"], reverse=True)

    return {"rows": unified, "facturas": facturas, "ops": ops}


def guardar_op(payload):
    """
    Guarda retenciones + facturas aplicadas a una OP.
    Si saldo == 0, marca las facturas aplicadas como pagadas en facturas.db.
    """
    try:
        op_id      = int(payload.get("op_id", 0))
        ret_g      = float(payload.get("ret_ganancias", 0) or 0)
        ret_i      = float(payload.get("ret_iibb",      0) or 0)
        # aplicaciones: lista de {factura_id, monto_aplicado}
        aplicaciones = payload.get("aplicaciones", [])

        # ── Actualizar retenciones en ordenes_pago.db ─────────────────
        conn_op = sqlite3.connect(DB_OP)
        conn_op.execute("""
            UPDATE ordenes_pago
            SET retencion_ganancias=?, retencion_iibb=?
            WHERE id=?
        """, (ret_g, ret_i, op_id))

        # ── Reemplazar vínculos op_facturas ───────────────────────────
        conn_op.execute("DELETE FROM op_facturas WHERE id_op=?", (op_id,))
        monto_aplicado_total = 0.0
        for ap in aplicaciones:
            fid   = int(ap["factura_id"])
            monto = float(ap["monto_aplicado"] or 0)
            conn_op.execute("""
                INSERT OR REPLACE INTO op_facturas (id_op, id_factura, monto_aplicado)
                VALUES (?, ?, ?)
            """, (op_id, fid, monto))
            monto_aplicado_total += monto

        # Recalcular saldo
        row = conn_op.execute(
            "SELECT monto_transferido FROM ordenes_pago WHERE id=?", (op_id,)
        ).fetchone()
        monto_op = (row[0] or 0) if row else 0
        saldo = monto_op - monto_aplicado_total + ret_g + ret_i
        conn_op.commit()
        conn_op.close()

        # ── Marcar facturas como pagadas si saldo == 0 ────────────────
        if abs(saldo) < 0.01 and aplicaciones:
            conn_f = sqlite3.connect(DB_FACT)
            for ap in aplicaciones:
                fid = int(ap["factura_id"])
                # solo marcar si aún no tiene se_pago
                existing = conn_f.execute(
                    "SELECT se_pago FROM facturas WHERE id=?", (fid,)
                ).fetchone()
                if existing and not existing[0]:
                    conn_f.execute(
                        "UPDATE facturas SET se_pago=? WHERE id=?",
                        (str(op_id), fid)
                    )
            conn_f.commit()
            conn_f.close()

        return {"success": True, "saldo": saldo, "marcadas_pagadas": abs(saldo) < 0.01}
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"success": False, "error": str(e)}


# ══════════════════════════════════════════════════════════════════════
# HTTP SERVER
# ══════════════════════════════════════════════════════════════════════

def send_html(handler, path):
    if not os.path.exists(path):
        handler.send_error(404, "Archivo no encontrado")
        return
    with open(path, "rb") as f:
        content = f.read()
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", len(content))
    handler.end_headers()
    handler.wfile.write(content)

def send_json(handler, obj):
    data = json.dumps(obj, default=str).encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", len(data))
    handler.end_headers()
    handler.wfile.write(data)

def parse_multipart(body, boundary):
    result = {}
    boundary_bytes = f"--{boundary}".encode()
    parts = body.split(boundary_bytes)
    for part in parts:
        if not part or part.startswith(b"--"):
            continue
        parts_split = part.split(b"\r\n\r\n", 1)
        if len(parts_split) == 2:
            headers, content = parts_split
            content = content.rstrip(b"\r\n")
            headers_text = headers.decode("utf-8", errors="ignore")
            if 'name="file"' in headers_text:
                result["file"] = content
            if 'name="check_dupes"' in headers_text:
                result["check_dupes"] = content.decode("utf-8", errors="ignore").strip()
    return result


class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"  [{self.address_string()}] {fmt % args}")

    def handle_error(self, request, client_address):
        import sys
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionAbortedError, BrokenPipeError, ConnectionResetError)):
            return
        super().handle_error(request, client_address)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path.rstrip("/") or "/"
        qs     = urllib.parse.parse_qs(parsed.query)

        # ----- INICIO DEL PEAJE -----
        # Rutas que NO necesitan login (los archivos estáticos o el propio login)
        rutas_publicas = ["/login"] 
        
        if path not in rutas_publicas and not path.startswith("/api/"):
            cookie_header = self.headers.get('Cookie')
            usuario_actual = auth.obtener_usuario_de_cookie(cookie_header)
            
            if not usuario_actual:
                # Si no tiene pase, lo mandamos a la pantalla de login
                self.send_response(302)
                self.send_header("Location", "/login")
                self.end_headers()
                return
        # ----- FIN DEL PEAJE -----

        # ── Páginas HTML ─────────────────────────────────────────────
        if path == "/login":
            return send_html(self, HTML_LOGIN)
            
        if path == "/":
            return send_html(self, HTML_PANEL)

        if path == "/admin":
            cookie_header = self.headers.get('Cookie')
            usuario_actual = auth.obtener_usuario_de_cookie(cookie_header)
            # Si no es el admin, lo pateamos al panel principal
            if usuario_actual != "admin":
                self.send_response(302)
                self.send_header("Location", "/")
                self.end_headers()
                return
            return send_html(self, HTML_ADMIN)

        if path in ("/facturas", "/fact", "/facturas/", "/fact/"):
            sync_transferencias_to_op()
            return send_html(self, HTML_FACT)
            
        if path in ("/trans", "/transferencias", "/trans/", "/transferencias/"):
            return send_html(self, HTML_TRANS)
            
        if path in ("/ops", "/cuentacorriente", "/cc"):
            sync_transferencias_to_op()
            return send_html(self, HTML_OPS)

        # ── API Sesiones / Admin ──────────────────────────────────────
        if path == "/api/sesiones":
            cookie_header = self.headers.get('Cookie')
            if auth.obtener_usuario_de_cookie(cookie_header) != "admin":
                self.send_error(403)
                return
            
            # Contamos cuántas sesiones abiertas tiene cada usuario
            from collections import Counter
            conteo = Counter(auth.sesiones_activas.values())
            data = [{"usuario": u, "conexiones": c} for u, c in conteo.items()]
            return send_json(self, data)

        # ── API Facturas ──────────────────────────────────────────────
        if path == "/facturas/api/stats":
            return send_json(self, facturas_stats())
        if path == "/facturas/api/data":
            return send_json(self, facturas_query(qs))
        if path == "/facturas/api/csv":
            data = facturas_csv(qs)
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="facturas.csv"')
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self.wfile.write(data)
            return
        if path == "/facturas/api/job":
            job_id = qs.get("id", [""])[0]
            with _jobs_lock:
                job = dict(_jobs.get(job_id, {}))
            if not job:
                return send_json(self, {"status": "unknown"})
            out = {"status": job["status"], "step": job.get("step",""), "pct": job.get("pct",0)}
            if job["status"] == "done":   out["result"] = job.get("result", {})
            if job["status"] == "error":  out["error"]  = job.get("error", "Error")
            return send_json(self, out)

        # ── API Transferencias ────────────────────────────────────────
        if path == "/trans/api/stats":
            return send_json(self, trans_stats())
        if path == "/trans/api/data":
            return send_json(self, trans_query(qs))
        if path == "/trans/api/csv":
            data = trans_csv(qs)
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="transferencias.csv"')
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self.wfile.write(data)
            return
        if path == "/trans/api/job":
            job_id = qs.get("id", [""])[0]
            with _jobs_lock:
                job = dict(_jobs.get(job_id, {}))
            if not job:
                return send_json(self, {"status": "unknown"})
            out = {"status": job["status"], "step": job.get("step",""), "pct": job.get("pct",0)}
            if job["status"] == "done":   out["result"] = job.get("result", {})
            if job["status"] == "error":  out["error"]  = job.get("error", "Error")
            return send_json(self, out)
        if path == "/trans/api/pdf-status":
            return send_json(self, {"total": 0, "rows": [], "status": "ok"})

        # ── API Cuenta Corriente / OPs ────────────────────────────────
        if path == "/ops/api/data":
            return send_json(self, cuenta_corriente(qs))
        if path == "/ops/api/stats":
            return send_json(self, ops_stats())
        if path == "/ops/api/cuenta_corriente":
            q = qs.get("q", [""])[0].strip()
            return send_json(self, cuenta_corriente_prestador(q))

        self.send_error(404, "Not found")
    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body           = self.rfile.read(content_length)
        content_type   = self.headers.get("Content-Type", "")
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path.rstrip("/")

        # ----- PROCESAR EL LOGIN -----
        if path == "/api/login":
            try:
                p = json.loads(body.decode("utf-8"))
                usuario = p.get("usuario")
                clave = p.get("clave")
                
                if auth.validar_credenciales(usuario, clave):
                    token = auth.crear_sesion(usuario)
                    # Mandamos el JSON de éxito y le inyectamos la Cookie al navegador
                    datos = json.dumps({"success": True}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Set-Cookie", f"session_id={token}; Path=/; HttpOnly")
                    self.send_header("Content-Length", len(datos))
                    self.end_headers()
                    self.wfile.write(datos)
                else:
                    self.send_response(401) # No autorizado
                    self.end_headers()
            except Exception as e:
                self.send_response(400)
                self.end_headers()
            return
        # ----- PATEAR USUARIO -----
        if path == "/api/patear":
            cookie_header = self.headers.get('Cookie')
            if auth.obtener_usuario_de_cookie(cookie_header) != "admin":
                self.send_error(403)
                return
            try:
                p = json.loads(body.decode("utf-8"))
                user_a_patear = p.get("usuario")
                # Llama a la función que borra las cookies
                borradas = auth.patear_usuario(user_a_patear) 
                return send_json(self, {"success": True, "borradas": borradas})
            except Exception:
                self.send_error(400)
            return
        # ----- FIN DEL LOGIN -----
        
        # ... acá sigue el resto de tus POST (/facturas/api/preview, etc)
        # ── Facturas ─────────────────────────────────────────────────
        if path == "/facturas/api/preview":
            if "multipart/form-data" in content_type:
                boundary   = content_type.split("boundary=")[-1].strip()
                parts      = parse_multipart(body, boundary)
                file_bytes = parts.get("file", b"")
                if file_bytes:
                    job_id      = _make_id()
                    check_dupes = parts.get("check_dupes", "1") != "0"
                    with _jobs_lock:
                        _jobs[job_id] = {"status":"running","step":"Iniciando…","pct":0,"result":None,"error":None}
                    t = threading.Thread(target=_process_excel_facturas,
                                         args=(job_id, file_bytes, check_dupes), daemon=True)
                    t.start()
                    return send_json(self, {"success": True, "job_id": job_id})
            return send_json(self, {"success": False, "message": "No se recibió archivo."})

        if path == "/facturas/api/confirm":
            try:
                p = json.loads(body.decode("utf-8"))
            except Exception:
                return send_json(self, {"success": False, "message": "Payload inválido."})
            ins, upd, skp, msg = confirm_facturas(p.get("token",""), p.get("update_dupes", False))
            return send_json(self, {"success": True, "message": msg,
                                    "inserted": ins, "updated": upd, "skipped": skp})

        if path == "/facturas/api/clear":
            conn = sqlite3.connect(DB_FACT)
            conn.execute("DELETE FROM facturas")
            conn.commit()
            conn.close()
            return send_json(self, {"success": True, "message": "Base de facturas vaciada."})

        # ── Transferencias ────────────────────────────────────────────
        if path == "/trans/api/preview":
            if "multipart/form-data" in content_type:
                boundary   = content_type.split("boundary=")[-1].strip()
                parts      = parse_multipart(body, boundary)
                file_bytes = parts.get("file", b"")
                if file_bytes:
                    job_id = _make_id()
                    with _jobs_lock:
                        _jobs[job_id] = {"status":"running","step":"Iniciando…","pct":0,"result":None,"error":None}
                    t = threading.Thread(target=_process_txt_trans,
                                         args=(job_id, file_bytes), daemon=True)
                    t.start()
                    return send_json(self, {"success": True, "job_id": job_id})
            return send_json(self, {"success": False, "message": "No se recibió archivo."})

        if path == "/trans/api/confirm":
            try:
                p = json.loads(body.decode("utf-8"))
            except Exception:
                return send_json(self, {"success": False, "message": "Payload inválido."})
            ins, upd, skp, msg = confirm_trans(p.get("token",""), p.get("update_dupes", False))
            sync_transferencias_to_op()
            return send_json(self, {"success": True, "message": msg,
                                    "inserted": ins, "updated": upd, "skipped": skp})

        if path == "/trans/api/clear":
            conn = sqlite3.connect(DB_TRANS)
            conn.execute("DELETE FROM transferencias")
            conn.commit()
            conn.close()
            return send_json(self, {"success": True, "message": "Base de transferencias vaciada."})

        # ── OPs ───────────────────────────────────────────────────────
        if path == "/ops/api/guardar_op":
            try:
                p = json.loads(body.decode("utf-8"))
            except Exception:
                return send_json(self, {"success": False, "message": "Payload inválido."})
            return send_json(self, guardar_op(p))

        self.send_error(404, "Not found")


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════
# Buscá esto al final del archivo y reemplazalo:
class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    setup_dbs()
    auth.setup_db() #
    sync_transferencias_to_op()

    def run(port):
        # CAMBIÁ 'ReuseTCPServer' por 'ThreadedTCPServer'
        with ThreadedTCPServer((HOST, port), Handler) as httpd:
            print(f"\n{'='*55}")
            print(f"  SERVIDOR MULTIHILO ACTIVO — http://{HOST}:{port}")
            print(f"{'='*55}")
            print(f"  Panel:          http://{HOST}:{port}/")
            print(f"  Facturas:       http://{HOST}:{port}/facturas")
            print(f"  Transferencias: http://{HOST}:{port}/trans")
            print(f"  Cta Corriente:  http://{HOST}:{port}/ops")
            print(f"  Ctrl+C para detener.")
            print(f"{'='*55}\n")
            httpd.serve_forever()

    for p in range(PORT, PORT + 10):
        try:
            run(p)
            break
        except OSError as e:
            if e.errno in (10048, 10013, 98):
                print(f"Puerto {p} ocupado, probando {p+1}…")
                continue
            raise
    else:
        print("No se encontró puerto disponible.")