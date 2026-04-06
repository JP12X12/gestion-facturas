import sqlite3
import config

# ══════════════════════════════════════════════════════════════════════
# CUENTA CORRIENTE / OPs
# ══════════════════════════════════════════════════════════════════════

def ops_stats():
    try:
        conn = sqlite3.connect(config.DB_OP)
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

    conn = sqlite3.connect(config.DB_OP)
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
    if not q:
        return {"rows": [], "facturas": [], "ops": []}

    like = f"%{q}%"

    facturas = []
    try:
        conn = sqlite3.connect(config.DB_FACT)
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
        pass

    ops = []
    try:
        conn = sqlite3.connect(config.DB_OP)
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
        pass

    unified = sorted(facturas + ops, key=lambda x: x["fecha_sort"], reverse=True)
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
            SET retencion_ganancias=?, retencion_iibb=?
            WHERE id=?
        """, (ret_g, ret_i, op_id))

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

        row = conn_op.execute("SELECT monto_transferido FROM ordenes_pago WHERE id=?", (op_id,)).fetchone()
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