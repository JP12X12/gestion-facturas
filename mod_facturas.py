import sqlite3
import io
import csv
import threading
import time
import random
import re
import config # <--- Importamos tus rutas centralizadas

# Traemos la ruta de la base de datos de facturas
DB_FACT = config.DB_FACT

# ── Variables de estado (Ahora cada módulo tiene las suyas propias) ──
_jobs      = {}
_jobs_lock = threading.Lock()
_pending      = {}
_pending_lock = threading.Lock()

def _make_id():
    return f"{int(time.time())}{random.randint(10000, 99999)}"

# ── Funciones Ayudantes (Helpers) ──
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

# ... (acá abajo sigue tu def facturas_stats() y el resto que ya tenés) ...
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

