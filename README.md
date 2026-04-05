# Sistema de Gestión de Facturas y Tesorería

Este proyecto es una aplicación web desarrollada en Python diseñada para automatizar y centralizar la gestión de facturas, órdenes de pago y transferencias. Está enfocada en optimizar los procesos administrativos de tesorería, permitiendo un manejo eficiente de bases de datos locales y generación de reportes.

## 🚀 Características

- **Panel Unificado:** Interfaz central para acceder a los distintos módulos del sistema.
- **Gestión de Comprobantes:** Módulos específicos para facturas, órdenes de pago y transferencias.
- **Autenticación Segura:** Sistema de login con manejo de sesiones y niveles de acceso.
- **Automatización:** Procesamiento de datos y administración de registros mediante scripts de Python.
- **Bases de Datos Locales:** Uso de SQLite para un almacenamiento ligero y rápido.

## 🛠️ Tecnologías utilizadas

* **Lenguaje:** Python 3.x
* **Backend:** Server unificado (Python nativo/Flask)
* **Frontend:** HTML5, CSS3 (con templates dinámicos)
* **Base de datos:** SQLite (.db)

## 📂 Estructura del Proyecto

* `server_unificado.py`: Servidor principal que gestiona las rutas y la lógica del sistema.
* `auth.py`: Lógica de autenticación y seguridad.
* `listador.py`: Script para procesar y listar registros de las bases de datos.
* `index_facturas.html`, `index_op.html`, `index_transferencias.html`: Módulos de interfaz de usuario.
* `crear_usuario.py` / `cambiar_clave.py`: Herramientas de administración de usuarios.

## 🔧 Instalación y Configuración

1. **Clonar el repositorio:**
   ```bash
   git clone [https://github.com/JP12X12/gestion-facturas.git](https://github.com/JP12X12/gestion-facturas.git)
   cd gestion-facturas


   Configuración de la Base de Datos:
El sistema está configurado para ignorar los archivos .db por seguridad. Al iniciar el servidor por primera vez, asegúrate de ejecutar los scripts de inicialización o contar con las tablas necesarias:

facturas.db

ordenes_pago.db

transferencias.db

usuarios.db

Ejecutar la aplicación:

Bash
python server_unificado.py
🔒 Seguridad y Privacidad
Este repositorio utiliza un archivo .gitignore para asegurar que la información sensible, como las bases de datos locales y archivos de caché de Python (__pycache__), no sean subidos al repositorio público.

Desarrollado por Juan Pablo Formento


---

### Cómo actualizarlo en tu GitHub:

1.  Abre el archivo `README.md` que tienes en tu carpeta local.
2.  Borra lo que tiene y pega el código de arriba.
3.  Guarda los cambios y ejecuta en tu terminal:

```bash
git add README.md
git commit -m "Update: README profesional con descripción del sistema"
git push
¿Por qué este diseño?

Contexto Profesional: Al mencionar que es para procesos de tesorería, le das un valor real al código.

Instrucciones Claros: Si alguien (o tú mismo en el futuro) descarga el proyecto, sabrá que necesita crear las bases de datos primero porque no se suben al repo.

Tecnologías: Resalta tus habilidades con Python, que es tu fuerte.