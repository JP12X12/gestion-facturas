import sqlite3
import hashlib
import secrets
import os
from http import cookies
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BD_DIR   = os.path.join(BASE_DIR, "bd")

DB_USUARIOS = os.path.join(BD_DIR, "usuarios.db")
# Diccionario en memoria para guardar quién está conectado: { "token123": "juan", "token456": "pedro" }
sesiones_activas = {}

def setup_db():
    """Crea la tabla de usuarios y un usuario admin por defecto si está vacía."""
    conn = sqlite3.connect(DB_USUARIOS)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT UNIQUE,
            password_hash TEXT
        )
    """)
    # Chequeamos si hay usuarios
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM usuarios")
    if cursor.fetchone()[0] == 0:
        # Si está vacía, creamos el usuario 'admin' con clave 'admin123'
        clave_hasheada = hashear_clave("admin123")
        conn.execute("INSERT INTO usuarios (usuario, password_hash) VALUES (?, ?)", ("admin", clave_hasheada))
        print("✓ usuarios.db lista. Usuario por defecto: admin / admin123")
    conn.commit()
    conn.close()

def hashear_clave(clave_plana):
    """Convierte la clave en un texto ilegible irreversible."""
    return hashlib.sha256(clave_plana.encode('utf-8')).hexdigest()

def validar_credenciales(usuario, clave_plana):
    """Compara la clave ingresada con la de la base de datos."""
    clave_ingresada_hash = hashear_clave(clave_plana)
    conn = sqlite3.connect(DB_USUARIOS)
    cursor = conn.cursor()
    cursor.execute("SELECT password_hash FROM usuarios WHERE usuario = ?", (usuario,))
    resultado = cursor.fetchone()
    conn.close()
    
    if resultado and resultado[0] == clave_ingresada_hash:
        return True
    return False

def crear_sesion(usuario):
    """Genera un token único y lo guarda en el diccionario."""
    token = secrets.token_hex(16) # Genera algo tipo 'a1b2c3d4...'
    sesiones_activas[token] = usuario
    return token

def obtener_usuario_de_cookie(cookie_header):
    """Lee el encabezado HTTP, busca la cookie 'session_id' y verifica si existe."""
    if not cookie_header:
        return None
    C = cookies.SimpleCookie()
    try:
        C.load(cookie_header)
        if "session_id" in C:
            token = C["session_id"].value
            return sesiones_activas.get(token) # Devuelve el nombre o None
    except Exception:
        pass
    return None

def patear_usuario(usuario_a_patear):
    """Busca todas las sesiones de ese usuario y las borra (Lo echa del sistema)."""
    tokens_a_borrar = [token for token, user in sesiones_activas.items() if user == usuario_a_patear]
    for t in tokens_a_borrar:
        del sesiones_activas[t]
    return len(tokens_a_borrar)

def registrar_usuario(nuevo_usuario, clave_plana):
    """Agrega un nuevo usuario a la base de datos de forma segura."""
    clave_hash = hashear_clave(clave_plana)
    conn = sqlite3.connect(DB_USUARIOS)
    try:
        # Intentamos insertarlo
        conn.execute("INSERT INTO usuarios (usuario, password_hash) VALUES (?, ?)", (nuevo_usuario, clave_hash))
        conn.commit()
        exito = True
    except sqlite3.IntegrityError:
        # Si tira este error, es porque el usuario (que es UNIQUE) ya existe
        exito = False 
    finally:
        conn.close()
    return exito
    
def cambiar_clave(usuario_objetivo, nueva_clave_plana):
    """Actualiza la contraseña de un usuario existente."""
    clave_hash = hashear_clave(nueva_clave_plana)
    conn = sqlite3.connect(DB_USUARIOS)
    cursor = conn.cursor()
    
    # Actualizamos el hash donde el usuario coincida
    cursor.execute("UPDATE usuarios SET password_hash = ? WHERE usuario = ?", (clave_hash, usuario_objetivo))
    filas_afectadas = cursor.rowcount
    
    conn.commit()
    conn.close()
    
    # Si filas_afectadas es mayor a 0, significa que el usuario existía y se cambió
    return filas_afectadas > 0