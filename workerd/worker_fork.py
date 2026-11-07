import os
import json
import time
import signal
import argparse
import logging
import resource

from worker import (
    HANDLERS,
    obtener_trabajo,
    actualizar_trabajo,
    recibir_notificacion
)

from database import initialize_database

from control_socket import (
    iniciar_servidor_socket,
    enviar_notificacion,
    SOCKET_PATH
)

TIMEOUT_SECONDS = 15
TERMINATION_GRACE_SECONDS = 5


def aplicar_limites():
    resource.setrlimit(resource.RLIMIT_CPU, (10, 12))
    resource.setrlimit(
        resource.RLIMIT_AS,
        (256 * 1024 * 1024, 256 * 1024 * 1024)
    )
    resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))


def procesar_con_fork(job):
    job_id = job["id"]

    logging.info("Creando proceso hijo para trabajo %s", job_id)

    read_fd, write_fd = os.pipe()

    try:
        pid = os.fork()
    except OSError as error:
        os.close(read_fd)
        os.close(write_fd)
        actualizar_trabajo(job_id, "failed", str(error))
        return

    if pid == 0:
        os.close(read_fd)

        try:
            aplicar_limites()

            handler = HANDLERS.get(job["job_type"])

            if handler is None:
                raise ValueError(
                    f"Tipo de trabajo no permitido: {job['job_type']}"
                )

            payload = json.loads(job["payload"])
            resultado = handler(payload, job_id)

            print(
                f"Trabajo {job_id} ejecutado por proceso hijo.",
                flush=True
            )
            print(resultado, flush=True)

            os.close(write_fd)
            os._exit(0)

        except BaseException as error:
            try:
                mensaje = str(error).encode(
                    "utf-8", errors="replace"
                )[:4000]
                os.write(write_fd, mensaje)
            finally:
                os.close(write_fd)
                os._exit(1)

    os.close(write_fd)

    inicio = time.monotonic()
    estado = None
    terminado = False
    expirado = False

    while time.monotonic() - inicio < TIMEOUT_SECONDS:
        resultado, estado = os.waitpid(pid, os.WNOHANG)

        if resultado == pid:
            terminado = True
            break

        time.sleep(0.1)

    if not terminado:
        expirado = True
        logging.warning(
            "Timeout del trabajo %s. Enviando SIGTERM.",
            job_id
        )

        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

        limite = time.monotonic() + TERMINATION_GRACE_SECONDS

        while time.monotonic() < limite:
            resultado, estado = os.waitpid(pid, os.WNOHANG)

            if resultado == pid:
                terminado = True
                break

            time.sleep(0.1)

        if not terminado:
            logging.warning(
                "Trabajo %s no terminó. Enviando SIGKILL.",
                job_id
            )

            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

            _, estado = os.waitpid(pid, 0)

    error = os.read(read_fd, 4096).decode(
        "utf-8", errors="replace"
    )
    os.close(read_fd)

    if expirado:
        error = (
            f"Timeout: el trabajo superó {TIMEOUT_SECONDS} segundos."
        )
        actualizar_trabajo(job_id, "failed", error)
        logging.error("Trabajo %s terminado por timeout.", job_id)
        print(f"Trabajo {job_id} terminado por timeout.")
        return

    if os.WIFEXITED(estado) and os.WEXITSTATUS(estado) == 0:
        actualizar_trabajo(job_id, "completed")
        logging.info(
            "Trabajo %s completado. PID hijo: %s",
            job_id,
            pid
        )
        print(f"Trabajo {job_id} completado.")
    else:
        if not error:
            error = f"Proceso hijo terminó anormalmente: {estado}"

        actualizar_trabajo(job_id, "failed", error)
        logging.error("Trabajo %s fallido: %s", job_id, error)
        print(f"Trabajo {job_id} fallido: {error}")


def iniciar_socket_proceso():
    pid = os.fork()

    if pid == 0:
        try:
            iniciar_servidor_socket(recibir_notificacion)
            os._exit(0)
        except BaseException:
            os._exit(1)

    return pid


def esperar_socket():
    for intento in range(30):
        try:
            respuesta = enviar_notificacion({
                "event": "daemon_ready"
            })

            if respuesta.get("status") == "received":
                print("Socket preparado correctamente.")
                return

        except (OSError, ValueError):
            time.sleep(0.1)

    raise RuntimeError("No se pudo iniciar el servidor socket.")


def ejecutar_worker(una_vez=False):
    initialize_database()

    socket_pid = None

    if not una_vez:
        socket_pid = iniciar_socket_proceso()

        try:
            esperar_socket()
        except Exception:
            os.kill(socket_pid, signal.SIGTERM)
            os.waitpid(socket_pid, 0)
            raise

    print("Worker Daemon con fork iniciado.")

    try:
        while True:
            job = obtener_trabajo()

            if job is not None:
                procesar_con_fork(job)

                if una_vez:
                    break
            else:
                if una_vez:
                    print("No hay trabajos pendientes.")
                    break

                time.sleep(2)

    except KeyboardInterrupt:
        print("Deteniendo Worker Daemon...")

    finally:
        if socket_pid is not None:
            try:
                os.kill(socket_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

            os.waitpid(socket_pid, 0)

            if os.path.exists(SOCKET_PATH):
                os.unlink(SOCKET_PATH)

            print("Proceso del socket detenido.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--once",
        action="store_true",
        help="Procesar un solo trabajo y finalizar"
    )

    args = parser.parse_args()

    ejecutar_worker(una_vez=args.once)
