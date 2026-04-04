import auth

print("\n=========================================")
print("   🔐  CAMBIO DE CONTRASEÑA - TESORERÍA")
print("=========================================\n")

usuario = input("Ingresá el nombre del usuario (ej: admin): ").strip()
nueva_clave = input("Ingresá la NUEVA contraseña: ").strip()

if usuario and nueva_clave:
    # Llamamos a la función para cambiarla en la base de datos
    if auth.cambiar_clave(usuario, nueva_clave):
        print(f"\n✅ ¡Listo! La contraseña de '{usuario}' fue actualizada.")
        
        # Opcional pero recomendado: Lo desconectamos para que tenga que usar la clave nueva
        auth.patear_usuario(usuario)
        print(f"🔒 Si '{usuario}' estaba conectado, fue desconectado por seguridad.")
    else:
        print(f"\n❌ Error: El usuario '{usuario}' no existe en la base de datos.")
else:
    print("\n⚠️  El usuario y la contraseña no pueden estar vacíos.")

print("\nPresioná Enter para salir...")
input()