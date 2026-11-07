
import os
import socket
import json

SOCKET_PATH = "/run/workerd/ctl.sock"


def enviar_notificacion(mensaje):
    cliente = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    try:
        cliente.connect(SOCKET_PATH)
        cliente.sendall(
            (json.dumps(mensaje) + "\n").encode("utf-8")
        )

        respuesta = cliente.recv(1024)
        return json.loads(respuesta.decode("utf-8"))

    finally:
        cliente.close()


def iniciar_servidor_socket(callback):
    if os.path.exists(SOCKET_PATH):
        os.unlink(SOCKET_PATH)

    servidor = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    servidor.bind(SOCKET_PATH)
    os.chmod(SOCKET_PATH, 0o660)
    servidor.listen(5)

    print(f"Socket escuchando en {SOCKET_PATH}")

    while True:
        conexion, _ = servidor.accept()

        with conexion:
            try:
                datos = conexion.recv(4096)
                mensaje = json.loads(datos.decode("utf-8"))

                callback(mensaje)

                respuesta = {
                    "status": "received"
                }

            except Exception as error:
                respuesta = {
                    "status": "error",
                    "message": str(error)
                }

            conexion.sendall(
                (json.dumps(respuesta) + "\n").encode("utf-8")
            )
