import sqlite3
import io
import csv
import threading
import time
import random
import re
import os
import config

# ── Variables de estado para los Jobs (importación en segundo plano) ──
_jobs      = {}
_jobs_lock = threading.Lock()
_pending      = {}
_pending_lock = threading.Lock()

def _make_id():
    return f"{int(time.time())}{random.randint(10000, 99999)}"

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

# ══════════════════════════════════════════════════════════════════════
# TRANSFERENCIAS - Lógica
# ══════════════════════════════════════════════════════════════════════

def trans_stats():
    try:
        conn = sqlite3.connect(config.DB_TRANS)
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

    # Convertimos la fecha que ingresa el usuario "DD/MM/YYYY" a "YYYYMMDD"
    def ymd(d):
        p = d.split("/")
        return (p[2]+p[1]+p[0]) if len(p)==3 else d

    # 🚀 LA MAGIA: Le enseñamos a SQLite a leer "DD/MM/YYYY" como "YYYYMMDD"
    fecha_sql = "substr(fecha_solicitud,7,4)||substr(fecha_solicitud,4,2)||substr(fecha_solicitud,1,2)"

    if fecha_desde:
        where += f" AND {fecha_sql} >= ?"
        args.append(ymd(fecha_desde))
    if fecha_hasta:
        where += f" AND {fecha_sql} <= ?"
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

    # 🚀 ORDENAMIENTO CORRECTO POR FECHA REAL
    if sort == "fecha_asc":
        order = f"{fecha_sql} ASC, id ASC"
    else:
        order = f"{fecha_sql} DESC, id DESC"

    conn = sqlite3.connect(config.DB_TRANS)
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

    archivos_pdf = []
    if os.path.exists(config.CARPETA_DOCUMENTOS):
        try:
            archivos_pdf = [f for f in os.listdir(config.CARPETA_DOCUMENTOS) if f.lower().endswith('.pdf')]
        except Exception:
            pass

    for r in rows:
        r["has_pdf"] = False 
        numero = str(r.get("numero") or "")
        nro_op = str(r.get("nro_orden_pago") or "")
        
        num_limpio = re.sub(r'\D', '', numero).lstrip("0")
        op_limpia  = re.sub(r'\D', '', nro_op).lstrip("0")
        
        buscar_num = num_limpio if len(num_limpio) > 2 else None
        buscar_op  = op_limpia if len(op_limpia) > 2 else None
        
        if buscar_num or buscar_op:
            for archivo in archivos_pdf:
                numeros_en_archivo = [n.lstrip("0") for n in re.findall(r'\d+', archivo)]
                if (buscar_num and buscar_num in numeros_en_archivo) or \
                   (buscar_op and buscar_op in numeros_en_archivo):
                    r["has_pdf"] = True
                    break

    return {"rows": rows, "total": total, "page": page,
            "total_pages": total_pages, "page_size": page_size}

def trans_csv(params_qs):
    conn = sqlite3.connect(config.DB_TRANS)
    conn.row_factory = sqlite3.Row
    
    # También arreglamos la descarga del CSV para que salga ordenado perfecto
    fecha_sql = "substr(fecha_solicitud,7,4)||substr(fecha_solicitud,4,2)||substr(fecha_solicitud,1,2)"
    
    rows = conn.execute(f"SELECT * FROM transferencias ORDER BY {fecha_sql} DESC").fetchall()
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
        upd("Leyendo archivo TXT…", 10)
        text = file_bytes.decode("utf-8", errors="ignore")
        lines = text.split("\n")

        upd("Procesando líneas…", 50)
        records = []
        
        re_fecha = re.compile(r'\d{2}/\d{2}/\d{4}')
        re_tipo_op = re.compile(r'(?:(\d+)\s+)?(Proveedores|Depósitos Judiciales|Haberes|Honorarios)', re.IGNORECASE)
        re_cuenta = re.compile(r'([A-Za-z0-9\.\s\-]+?)\s*-\s*C[AC]\s*-\s*[\$\w]+\s*-\s*(\d+)\s*-\s*(\d+)\s*-\s*(\d+)\s*-\s*(.*?)(?=\s{2,}|\s*\$|$)')
        re_fin = re.compile(r'\$\s+([\d\.,]+)\s+([A-Za-z]+)\s*$')
        re_fin2 = re.compile(r'([\d\.,]+)\s+([A-Za-z]+)\s*$')

        for raw_line in lines:
            line = raw_line.strip()
            if not line or not re.match(r'^\d', line):
                continue 
            
            parts = line.split()
            numero = parts[0]
            
            m_fecha = re_fecha.search(line)
            fecha = m_fecha.group(0) if m_fecha else ""
            
            m_tipo = re_tipo_op.search(line)
            nro_op = m_tipo.group(1) if m_tipo and m_tipo.group(1) else ""
            tipo_trans = m_tipo.group(2) if m_tipo else "Transferencia"
            
            cuentas = re_cuenta.findall(line)
            
            cuenta_debito = cuenta_credito = cc_banco = cc_cbu = cc_cuit = cc_nombre = ""
            
            if len(cuentas) >= 1:
                cuenta_debito = " - ".join(cuentas[0])
            if len(cuentas) >= 2:
                cred = cuentas[1]
                cc_banco = cred[0].strip()
                cuenta_credito = cred[1].strip()
                cc_cuit = cred[2].strip()
                cc_cbu = cred[3].strip()
                cc_nombre = cred[4].strip()
                
            m_fin = re_fin.search(line)
            importe_str = "0"
            estado = "Desconocido"
            
            if m_fin:
                importe_str = m_fin.group(1)
                estado = m_fin.group(2)
            else:
                m_fin2 = re_fin2.search(line)
                if m_fin2:
                    importe_str = m_fin2.group(1)
                    estado = m_fin2.group(2)
            
            records.append({
                "numero": numero,
                "numero_red": parts[1] if len(parts) > 1 and parts[1].count(".") >= 1 else "",
                "fecha_solicitud": fecha,
                "comunidad": "",
                "nro_orden_pago": nro_op,
                "nro_pago": "",
                "tipo_transferencia": tipo_trans,
                "cuenta_debito": cuenta_debito,
                "cuenta_credito": cuenta_credito,
                "cc_banco": cc_banco,
                "cc_cbu": cc_cbu,
                "cc_cuit": cc_cuit,
                "cc_nombre": cc_nombre,
                "moneda": "$",
                "importe": parse_importe(importe_str),
                "estado": estado
            })

        if not records:
            raise Exception("No se detectaron registros válidos en el archivo TXT.")

        upd(f"Buscando duplicados… ({len(records)} registros)", 70)
        
        conn = sqlite3.connect(config.DB_TRANS)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT numero, estado, importe FROM transferencias")
        existing_map = {r["numero"]: dict(r) for r in c.fetchall()}
        conn.close()

        new_recs = []
        dup_recs = []

        for r in records:
            if r["numero"] in existing_map:
                dup_recs.append({"incoming": r, "existing": existing_map[r["numero"]]})
            else:
                new_recs.append(r)

        token = _make_id()
        with _pending_lock:
            _pending[token] = {"new": new_recs, "dupes": dup_recs, "type": "trans"}

        upd("Listo", 100)
        with _jobs_lock:
            _jobs[job_id].update({
                "status": "done",
                "result": {
                    "nuevos": len(new_recs),
                    "duplicados": len(dup_recs),
                    "total": len(records),
                    "token": token,
                    "dupes_preview": [
                        {
                            "numero": d["incoming"]["numero"],
                            "nro_op": d["incoming"]["nro_orden_pago"],
                            "nombre": d["incoming"]["cc_nombre"],
                            "estado_old": d["existing"].get("estado", ""),
                            "estado_new": d["incoming"]["estado"],
                            "importe_old": d["existing"].get("importe", ""),
                            "importe_new": d["incoming"]["importe"]
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

def confirm_trans(token, update_dupes=False):
    with _pending_lock:
        data = _pending.pop(token, None)
    if not data:
        return 0, 0, 0, "Token inválido."

    new_recs = data.get("new", [])
    dup_recs = data.get("dupes", [])
    
    conn = sqlite3.connect(config.DB_TRANS)
    ins, upd, skp = 0, 0, 0

    def armar_tupla(r):
        return (
            r["numero"], r["numero_red"], r["fecha_solicitud"], r["comunidad"],
            r["nro_orden_pago"], r["nro_pago"], r["tipo_transferencia"],
            r["cuenta_debito"], r["cuenta_credito"], r["cc_banco"],
            r["cc_cbu"], r["cc_cuit"], r["cc_nombre"], r["moneda"],
            r["importe"], r["estado"]
        )

    query_insert = """
        INSERT INTO transferencias
        (numero, numero_red, fecha_solicitud, comunidad, nro_orden_pago, nro_pago,
         tipo_transferencia, cuenta_debito, cuenta_credito, cc_banco, cc_cbu,
         cc_cuit, cc_nombre, moneda, importe, estado)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for r in new_recs:
        try:
            conn.execute(query_insert, armar_tupla(r))
            ins += 1
        except Exception:
            skp += 1
            
    if update_dupes:
        query_update = """
            UPDATE transferencias
            SET numero_red=?, fecha_solicitud=?, comunidad=?, nro_orden_pago=?, nro_pago=?,
                tipo_transferencia=?, cuenta_debito=?, cuenta_credito=?, cc_banco=?, cc_cbu=?,
                cc_cuit=?, cc_nombre=?, moneda=?, importe=?, estado=?
            WHERE numero=?
        """
        for d in dup_recs:
            r = d["incoming"]
            try:
                conn.execute(query_update, (
                    r["numero_red"], r["fecha_solicitud"], r["comunidad"],
                    r["nro_orden_pago"], r["nro_pago"], r["tipo_transferencia"],
                    r["cuenta_debito"], r["cuenta_credito"], r["cc_banco"],
                    r["cc_cbu"], r["cc_cuit"], r["cc_nombre"], r["moneda"],
                    r["importe"], r["estado"], r["numero"]
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