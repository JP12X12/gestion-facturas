import os

# Raíz del proyecto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Subcarpetas
HTML_DIR = os.path.join(BASE_DIR, "htmls")
BD_DIR   = os.path.join(BASE_DIR, "bd")
CARPETA_DOCUMENTOS = r"C:\Users\Juan\Desktop\Trabajo\Proyectos\Panel unificado\archivo\comprobantes" 

os.makedirs(HTML_DIR, exist_ok=True)
os.makedirs(BD_DIR, exist_ok=True)

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