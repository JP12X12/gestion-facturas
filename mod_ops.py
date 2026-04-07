import sqlite3
import config

def to_yyyymmdd(d_str):
    """
    Toma cualquier fecha (DD/MM/YYYY, YYYY-MM-DD, etc) 
    y la convierte a YYYYMMDD para que Python la pueda ordenar perfecto de mayor a menor.
    """
    if not d_str: return "00000000"
    d_str = str(d_str).strip().split(" ")[0]
    if "-" in d_str:
        p = d_str.split("-")
        if len(p) == 3:
            return p[0]+p[1]+p[2] if len(p[0]) == 4 else p[2]+p[1]+p[0]
    if "/" in d_str:
        p = d_str.split("/")
        if len(p) == 3:
            return p[0]+p[1]+p[2] if len(p[0]) == 4 else p[2]+p[1]+p[0]
    return d_str

# ══════════════════════════════════════════════════════════════════════
# CUENTA CORRIENTE / OPs
# ══════════════════════════════════════════════════════════════════════
def ops_stats():
    """Devuelve las estadísticas para que la tarjeta del Panel principal marque Online"""
    try:
        conn = sqlite3.connect(config.DB_OP)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM ordenes_pago")
        total = c.fetchone()[0] or 0
        # Ahora usa 'monto' en lugar del viejo 'monto_transferido'
        c.execute("SELECT COALESCE(SUM(monto),0) FROM ordenes_pago")
        total_monto = c.fetchone()[0] or 0
        conn.close()
        return {"total": total, "total_monto": total_monto}
    except Exception:
        return {"total": 0, "total_monto": 0}
def buscar_prestadores(q):
    """Devuelve una lista única de {cuit, nombre} buscando en ambas DBs"""
    if not q:
        return []
        
    like = f"%{q}%"
    prestadores = {}

    try:
        conn = sqlite3.connect(config.DB_FACT)
        rows = conn.execute("""
            SELECT DISTINCT nro_doc_emisor, denominacion_emisor 
            FROM facturas 
            WHERE nro_doc_emisor LIKE ? OR denominacion_emisor LIKE ?
        """, (like, like)).fetchall()
        for r in rows:
            cuit = str(r[0] or "").strip()
            if cuit:
                prestadores[cuit] = r[1] or ""
        conn.close()
    except Exception:
        pass

    try:
        conn = sqlite3.connect(config.DB_OP)
        rows = conn.execute("""
            SELECT DISTINCT cuit, beneficiario 
            FROM ordenes_pago 
            WHERE cuit LIKE ? OR beneficiario LIKE ?
        """, (like, like)).fetchall()
        for r in rows:
            cuit = str(r[0] or "").strip()
            if cuit and cuit not in prestadores: 
                prestadores[cuit] = r[1] or ""
        conn.close()
    except Exception:
        pass

    return [{"cuit": k, "nombre": v} for k, v in prestadores.items()]


def cuenta_corriente_prestador(cuit):
    """Devuelve facturas y OPs exactas para el CUIT seleccionado"""
    if not cuit:
        return {"rows": [], "facturas": [], "ops": []}

    facturas = []
    try:
        conn = sqlite3.connect(config.DB_FACT)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT id, fecha_emision, tipo_comprobante, punto_venta, numero_desde,
                   imp_total, importe, se_pago, denominacion_emisor, nro_doc_emisor, factura, Estado
            FROM facturas
            WHERE nro_doc_emisor = ?
        """, (cuit,)).fetchall()
        
        for r in rows:
            se_pago = str(r["se_pago"] or "").strip()
            estado_db = str(r["Estado"] or "").strip().upper()
            
            if se_pago:
                estado_calc = "Pagada"
            elif estado_db == "PAG":
                estado_calc = "Pagada sin aplicar"
            elif estado_db == "AUD":
                estado_calc = "En auditoría"
            elif estado_db == "PEP":
                estado_calc = "Pendiente"
            else:
                estado_calc = "Pendiente" 

            facturas.append({
                "tipo":          "factura",
                "id":            r["id"],
                "fecha":         r["fecha_emision"] or "",
                "fecha_sort":    to_yyyymmdd(r["fecha_emision"]), # <--- ESTANDARIZADO
                "punto_venta":   r["punto_venta"]   or "",
                "numero_desde":  r["numero_desde"]  or "",
                "factura":       r["factura"]        or "",
                "imp_total":     r["imp_total"]      or 0,
                "importe":       r["importe"]        or 0,
                "estado_calc":   estado_calc,
                "denominacion":  r["denominacion_emisor"] or "",
                "cuit":          r["nro_doc_emisor"] or "",
            })
        conn.close()
    except Exception:
        pass

    ops = []
    try:
        conn = sqlite3.connect(config.DB_OP)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT id, fecha, numero_op, beneficiario, cuit,
                   monto, ret_ganancias, ret_iibb
            FROM ordenes_pago
            WHERE cuit = ?
        """, (cuit,)).fetchall()
        for r in rows:
            fact_ids = [x[0] for x in conn.execute(
                "SELECT factura_id FROM op_facturas WHERE op_id=?", (r["id"],)
            ).fetchall()]
            montos_aplicados = {x[0]: x[1] for x in conn.execute(
                "SELECT factura_id, monto_aplicado FROM op_facturas WHERE op_id=?", (r["id"],)
            ).fetchall()}
            
            monto_aplicado_total = sum(montos_aplicados.values())
            ret_g = r["ret_ganancias"] or 0
            ret_i = r["ret_iibb"]      or 0
            saldo = (r["monto"] or 0) - monto_aplicado_total + ret_g + ret_i
            
            fecha = r["fecha"] or ""
            ops.append({
                "tipo":          "op",
                "id":            r["id"],
                "fecha":         fecha,
                "fecha_sort":    to_yyyymmdd(fecha), # <--- ESTANDARIZADO
                "numero_op":     r["numero_op"]      or "",
                "monto":         r["monto"]          or 0,
                "ret_ganancias": ret_g,
                "ret_iibb":      ret_i,
                "estado":        "generada",
                "nombre":        r["beneficiario"]   or "",
                "cuit":          r["cuit"]           or "",
                "fact_ids":      fact_ids,
                "montos_aplicados": montos_aplicados,
                "monto_aplicado_total": monto_aplicado_total,
                "saldo":         saldo,
            })
        conn.close()
    except Exception:
        pass

    # ORDENAMIENTO FINAL (De más actual a más antigua)
    facturas.sort(key=lambda x: x["fecha_sort"], reverse=True) # Acomoda el Modal
    ops.sort(key=lambda x: x["fecha_sort"], reverse=True)
    unified = sorted(facturas + ops, key=lambda x: x["fecha_sort"], reverse=True) # Acomoda la Tabla Principal
    
    return {"rows": unified, "facturas": facturas, "ops": ops}

def guardar_op(payload):
    try:
        op_id      = int(payload.get("op_id", 0))
        ret_g      = float(payload.get("ret_ganancias", 0) or 0)
        ret_i      = float(payload.get("ret_iibb",      0) or 0)
        aplicaciones = payload.get("aplicaciones", [])

        conn_op = sqlite3.connect(config.DB_OP)
        conn_op.execute("""
            UPDATE ordenes_pago
            SET ret_ganancias=?, ret_iibb=?
            WHERE id=?
        """, (ret_g, ret_i, op_id))

        conn_op.execute("DELETE FROM op_facturas WHERE op_id=?", (op_id,))
        monto_aplicado_total = 0.0
        for ap in aplicaciones:
            fid   = int(ap["factura_id"])
            monto = float(ap["monto_aplicado"] or 0)
            conn_op.execute("""
                INSERT OR REPLACE INTO op_facturas (op_id, factura_id, monto_aplicado)
                VALUES (?, ?, ?)
            """, (op_id, fid, monto))
            monto_aplicado_total += monto

        row = conn_op.execute("SELECT monto FROM ordenes_pago WHERE id=?", (op_id,)).fetchone()
        monto_op = (row[0] or 0) if row else 0
        saldo = monto_op - monto_aplicado_total + ret_g + ret_i
        conn_op.commit()
        conn_op.close()

        if abs(saldo) < 0.01 and aplicaciones:
            conn_f = sqlite3.connect(config.DB_FACT)
            for ap in aplicaciones:
                fid = int(ap["factura_id"])
                existing = conn_f.execute("SELECT se_pago FROM facturas WHERE id=?", (fid,)).fetchone()
                if existing and not existing[0]:
                    conn_f.execute("UPDATE facturas SET se_pago=? WHERE id=?", (str(op_id), fid))
            conn_f.commit()
            conn_f.close()

        return {"success": True, "saldo": saldo, "marcadas_pagadas": abs(saldo) < 0.01}
    except Exception as e:
        return {"success": False, "error": str(e)}