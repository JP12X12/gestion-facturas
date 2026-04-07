#!/usr/bin/env python3
"""
Servidor unificado DEV - Arquitectura Modular
"""
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
import socket
import mimetypes

# ── NUESTROS MÓDULOS ──
import config
import auth
import base_datos
import mod_facturas 
import mod_trans
import mod_ops
HOST = "0.0.0.0"
PORT = 5001 # Puerto de Desarrollo


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
    parts = {}
    delimiter = b"--" + boundary.encode("utf-8")
    chunks = body.split(delimiter)
    for chunk in chunks:
        if not chunk or chunk == b"--\r\n" or chunk == b"--": continue
        if b"\r\n\r\n" in chunk:
            headers_part, content_part = chunk.split(b"\r\n\r\n", 1)
            content_part = content_part.rstrip(b"\r\n")
            if b'name="file"' in headers_part:
                parts["file"] = content_part
            elif b'name="check_dupes"' in headers_part:
                parts["check_dupes"] = content_part.decode("utf-8", errors="ignore")
    return parts

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass 

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path.rstrip("/") or "/"
        qs     = urllib.parse.parse_qs(parsed.query)

        # ----- INICIO DEL PEAJE -----
        rutas_publicas = ["/login"] 
        if path not in rutas_publicas and not path.startswith("/api/"):
            cookie_header = self.headers.get('Cookie')
            usuario_actual = auth.obtener_usuario_de_cookie(cookie_header)
            
            if not usuario_actual:
                self.send_response(302)
                self.send_header("Location", "/login")
                self.end_headers()
                return
        # ----- FIN DEL PEAJE -----

        # ── Páginas HTML ─────────────────────────
        if path == "/login":
            return send_html(self, config.HTML_LOGIN)
        if path == "/":
            return send_html(self, config.HTML_PANEL)
        if path == "/admin":
            cookie_header = self.headers.get('Cookie')
            if auth.obtener_usuario_de_cookie(cookie_header) != "admin":
                self.send_response(302)
                self.send_header("Location", "/")
                self.end_headers()
                return
            return send_html(self, config.HTML_ADMIN)
        if path in ("/facturas", "/fact", "/facturas/", "/fact/"):
            base_datos.sync_transferencias_to_op()
            return send_html(self, config.HTML_FACT)
        if path in ("/trans", "/transferencias", "/trans/", "/transferencias/"):
            return send_html(self, config.HTML_TRANS)
        if path in ("/ops", "/cuentacorriente", "/cc"):
            base_datos.sync_transferencias_to_op()
            return send_html(self, config.HTML_OPS)

        # ── API Sesiones / Admin ──────────────────────────────────────
        if path == "/api/sesiones":
            cookie_header = self.headers.get('Cookie')
            if auth.obtener_usuario_de_cookie(cookie_header) != "admin":
                self.send_error(403)
                return
            from collections import Counter
            conteo = Counter(auth.sesiones_activas.values())
            data = [{"usuario": u, "conexiones": c} for u, c in conteo.items()]
            return send_json(self, data)

        # ── API Facturas ──────────────────────────────────────────────
        if path == "/facturas/api/stats":
            return send_json(self, mod_facturas.facturas_stats())
        if path == "/facturas/api/data":
            return send_json(self, mod_facturas.facturas_query(qs))
        if path == "/facturas/api/csv":
            data = mod_facturas.facturas_csv(qs)
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="facturas.csv"')
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self.wfile.write(data)
            return
        if path == "/facturas/api/job":
            job_id = qs.get("id", [""])[0]
            with mod_facturas._jobs_lock:
                job = dict(mod_facturas._jobs.get(job_id, {}))
            if not job:
                return send_json(self, {"status": "unknown"})
            out = {"status": job["status"], "step": job.get("step",""), "pct": job.get("pct",0)}
            if job["status"] == "done":   out["result"] = job.get("result", {})
            if job["status"] == "error":  out["error"]  = job.get("error", "Error")
            return send_json(self, out)

        # ── API Transferencias ────────────────────────────────────────
        if path == "/trans/api/stats":
            return send_json(self, mod_trans.trans_stats())
        if path == "/trans/api/data":
            return send_json(self, mod_trans.trans_query(qs))
        if path == "/trans/api/csv":
            data = mod_trans.trans_csv(qs)
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="transferencias.csv"')
            self.send_header("Content-Length", len(data))
            self.end_headers()
            self.wfile.write(data)
            return
        if path == "/trans/api/job":
            job_id = qs.get("id", [""])[0]
            with mod_trans._jobs_lock:
                job = dict(mod_trans._jobs.get(job_id, {}))
            if not job:
                return send_json(self, {"status": "unknown"})
            out = {"status": job["status"], "step": job.get("step",""), "pct": job.get("pct",0)}
            if job["status"] == "done":   out["result"] = job.get("result", {})
            if job["status"] == "error":  out["error"]  = job.get("error", "Error")
            return send_json(self, out)

        # ── API Documentos (Comprobantes PDF) ─────────────────────────
        if path == "/trans/api/pdf-status":
            if os.path.exists(config.CARPETA_DOCUMENTOS):
                try:
                    archivos = [f for f in os.listdir(config.CARPETA_DOCUMENTOS) if f.lower().endswith('.pdf')]
                    return send_json(self, {"exists": True, "count": len(archivos), "dir": config.CARPETA_DOCUMENTOS})
                except Exception:
                    return send_json(self, {"exists": True, "count": 0, "dir": config.CARPETA_DOCUMENTOS})
            else:
                return send_json(self, {"exists": False, "count": 0, "dir": config.CARPETA_DOCUMENTOS})

        if path == "/trans/api/pdf":
            numero = qs.get("numero", [""])[0]
            nro_op = qs.get("nro_op", [""])[0]
            
            num_limpio = re.sub(r'\D', '', numero).lstrip("0")
            op_limpia  = re.sub(r'\D', '', nro_op).lstrip("0")
            
            buscar_num = num_limpio if len(num_limpio) > 2 else None
            buscar_op  = op_limpia if len(op_limpia) > 2 else None
            
            archivo_encontrado = None
            if os.path.exists(config.CARPETA_DOCUMENTOS):
                for f in os.listdir(config.CARPETA_DOCUMENTOS):
                    if not f.lower().endswith('.pdf'):
                        continue
                    numeros_en_archivo = [n.lstrip("0") for n in re.findall(r'\d+', f)]
                    if (buscar_num and buscar_num in numeros_en_archivo) or \
                       (buscar_op and buscar_op in numeros_en_archivo):
                        archivo_encontrado = os.path.join(config.CARPETA_DOCUMENTOS, f)
                        break
            
            if archivo_encontrado:
                with open(archivo_encontrado, 'rb') as f:
                    contenido = f.read()
                tipo_mime, _ = mimetypes.guess_type(archivo_encontrado)
                self.send_response(200)
                self.send_header("Content-Type", tipo_mime or "application/pdf")
                self.send_header("Content-Length", len(contenido))
                self.send_header("Content-Disposition", f'inline; filename="{os.path.basename(archivo_encontrado)}"')
                self.end_headers()
                self.wfile.write(contenido)
            else:
                self.send_error(404, "Comprobante no encontrado en la carpeta")
            return

        # ── API Cuenta Corriente / OPs ────────────────────────────────
        if path == "/ops/api/stats":
            return send_json(self, mod_ops.ops_stats())
            
        if path == "/ops/api/buscar_prestadores":
            q = qs.get("q", [""])[0].strip()
            return send_json(self, mod_ops.buscar_prestadores(q))
            
        if path == "/ops/api/cuenta_corriente":
            cuit = qs.get("cuit", [""])[0].strip()
            return send_json(self, mod_ops.cuenta_corriente_prestador(cuit))

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
                    datos = json.dumps({"success": True}).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Set-Cookie", f"session_id={token}; Path=/; HttpOnly")
                    self.send_header("Content-Length", len(datos))
                    self.end_headers()
                    self.wfile.write(datos)
                else:
                    self.send_response(401)
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
                borradas = auth.patear_usuario(user_a_patear) 
                return send_json(self, {"success": True, "borradas": borradas})
            except Exception:
                self.send_error(400)
            return
            
        # ── Facturas ─────────────────────────────────────────────────
        if path == "/facturas/api/preview":
            if "multipart/form-data" in content_type:
                boundary   = content_type.split("boundary=")[-1].strip()
                parts      = parse_multipart(body, boundary)
                file_bytes = parts.get("file", b"")
                if file_bytes:
                    job_id      = mod_facturas._make_id()
                    check_dupes = parts.get("check_dupes", "1") != "0"
                    with mod_facturas._jobs_lock:
                        mod_facturas._jobs[job_id] = {"status":"running","step":"Iniciando…","pct":0,"result":None,"error":None}
                    t = threading.Thread(target=mod_facturas._process_excel_facturas,
                                         args=(job_id, file_bytes, check_dupes), daemon=True)
                    t.start()
                    return send_json(self, {"success": True, "job_id": job_id})
            return send_json(self, {"success": False, "message": "No se recibió archivo."})

        if path == "/facturas/api/confirm":
            try:
                p = json.loads(body.decode("utf-8"))
            except Exception:
                return send_json(self, {"success": False, "message": "Payload inválido."})
            ins, upd, skp, msg = mod_facturas.confirm_facturas(p.get("token",""), p.get("update_dupes", False))
            return send_json(self, {"success": True, "message": msg,
                                    "inserted": ins, "updated": upd, "skipped": skp})

        if path == "/facturas/api/clear":
            conn = sqlite3.connect(config.DB_FACT)
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
                    job_id = mod_trans._make_id()
                    with mod_trans._jobs_lock:
                        mod_trans._jobs[job_id] = {"status":"running","step":"Iniciando…","pct":0,"result":None,"error":None}
                    t = threading.Thread(target=mod_trans._process_txt_trans,
                                        args=(job_id, file_bytes), daemon=True)
                    t.start()
                    return send_json(self, {"success": True, "job_id": job_id})
            return send_json(self, {"success": False, "message": "No se recibió archivo."})

        if path == "/trans/api/confirm":
            try:
                p = json.loads(body.decode("utf-8"))
            except Exception:
                return send_json(self, {"success": False, "message": "Payload inválido."})
            ins, upd, skp, msg = mod_trans.confirm_trans(p.get("token",""), p.get("update_dupes", False))
            base_datos.sync_transferencias_to_op()
            return send_json(self, {"success": True, "message": msg,
                                    "inserted": ins, "updated": upd, "skipped": skp})

        if path == "/trans/api/clear":
            conn = sqlite3.connect(config.DB_TRANS)
            conn.execute("DELETE FROM transferencias")
            conn.commit()
            conn.close()
            return send_json(self, {"success": True, "message": "Base de transferencias vaciada."})
        if path == "/trans/api/confirm":
            try:
                p = json.loads(body.decode("utf-8"))
            except Exception:
                return send_json(self, {"success": False, "message": "Payload inválido."})
            ins, upd, skp, msg = mod_trans.confirm_trans(p.get("token",""), p.get("update_dupes", False))
            base_datos.sync_transferencias_to_op()
            return send_json(self, {"success": True, "message": msg,
                                    "inserted": ins, "updated": upd, "skipped": skp})

        if path == "/trans/api/clear":
            conn = sqlite3.connect(config.DB_TRANS)
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
            return send_json(self, mod_ops.guardar_op(p))
        self.send_error(404, "Not found")

# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════
class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    base_datos.setup_dbs()             
    auth.setup_db() 
    base_datos.sync_transferencias_to_op() 

    def run(port):
        with ThreadedTCPServer((HOST, port), Handler) as httpd:
            print(f"\n{'='*55}")
            print(f"  SERVIDOR MULTIHILO ACTIVO — http://localhost:{port}")
            print(f"{'='*55}")
            print(f"  Panel:          http://localhost:{port}/")
            print(f"  Facturas:       http://localhost:{port}/facturas")
            print(f"  Transferencias: http://localhost:{port}/trans")
            print(f"  Cta Corriente:  http://localhost:{port}/ops")
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