import auth

print("\n=========================================")
print("   🗝️  CREADOR DE USUARIOS - TESORERÍA")
print("=========================================\n")

usuario = input("Ingresá el nombre del nuevo usuario: ").strip()
clave = input("Ingresá la contraseña para este usuario: ").strip()

if usuario and clave:
    # Llamamos a la función que acabás de agregar en auth.py
    if auth.registrar_usuario(usuario, clave):
        print(f"\n✅ ¡Éxito! El usuario '{usuario}' ya puede iniciar sesión.")
    else:
        print(f"\n❌ Error: El usuario '{usuario}' ya existe en el sistema.")
else:
    print("\n⚠️  El usuario y la contraseña no pueden estar vacíos.")

print("\nPresioná Enter para salir...")
input()