import os
import json
import time
import logging
import argparse

import threading
from control_socket import iniciar_servidor_socket
from database import get_connection, initialize_database

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
LOGS_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOGS_DIR, "worker.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def ejecutar_reporte(payload, job_id):
    if not isinstance(payload, dict):
        raise ValueError("El payload debe ser un objeto JSON")

    mensaje = payload.get("mensaje", "Reporte generado")
    duracion = payload.get("duracion", 0)

    if not isinstance(duracion, (int, float)) or duracion < 0:
        raise ValueError("La duración debe ser un número positivo")

    time.sleep(min(duracion, 30))

    contenido = (
        "REPORTE DEL WORKER DAEMON\n"
        "=========================\n"
        f"ID del trabajo: {job_id}\n"
        f"Mensaje: {mensaje}\n"
        f"Duración solicitada: {duracion} segundos\n"
        "Estado: Ejecutado correctamente\n"
    )

    ruta = os.path.join(RESULTS_DIR, f"reporte_{job_id}.txt")

    with open(ruta, "w", encoding="utf-8") as archivo:
        archivo.write(contenido)

    return contenido


HANDLERS = {
    "reporte": ejecutar_reporte
}

def recibir_notificacion(mensaje):
    print("Notificación recibida:", mensaje)
    logging.info("Notificación recibida: %s", mensaje)
def obtener_trabajo():
    conn = get_connection()

    try:
        conn.execute("BEGIN IMMEDIATE")

        job = conn.execute("""
            SELECT *
            FROM jobs
            WHERE status = 'pending'
            ORDER BY priority DESC, id ASC
            LIMIT 1
        """).fetchone()

        if job is None:
            conn.commit()
            return None

        cursor = conn.execute("""
            UPDATE jobs
            SET status = 'running',
                attempts = attempts + 1,
                started_at = CURRENT_TIMESTAMP,
                error_message = NULL
            WHERE id = ? AND status = 'pending'
        """, (job["id"],))

        if cursor.rowcount != 1:
            conn.rollback()
            return None

        conn.commit()

        return dict(job)

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


def actualizar_trabajo(job_id, status, error=None):
    conn = get_connection()

    conn.execute("""
        UPDATE jobs
        SET status = ?,
            completed_at = CURRENT_TIMESTAMP,
            error_message = ?
        WHERE id = ?
    """, (status, error, job_id))

    conn.commit()
    conn.close()


def procesar_trabajo(job):
    job_id = job["id"]
    job_type = job["job_type"]

    logging.info("Iniciando trabajo %s de tipo %s", job_id, job_type)

    try:
        handler = HANDLERS.get(job_type)

        if handler is None:
            raise ValueError(f"Tipo de trabajo no permitido: {job_type}")

        payload = json.loads(job["payload"])

        resultado = handler(payload, job_id)

        actualizar_trabajo(job_id, "completed")

        logging.info("Trabajo %s completado correctamente", job_id)

        print(f"Trabajo {job_id} completado.")
        print(resultado)

    except Exception as error:
        actualizar_trabajo(job_id, "failed", str(error))

        logging.exception("Error procesando trabajo %s", job_id)

        print(f"Error en el trabajo {job_id}: {error}")


def ejecutar_worker(una_vez=False):
    initialize_database()

    if not una_vez:
        hilo_socket = threading.Thread(
            target=iniciar_servidor_socket,
            args=(recibir_notificacion,),
            daemon=True
        )
        hilo_socket.start()

        time.sleep(0.5)

    print("Worker Daemon iniciado.")

    while True:
        job = obtener_trabajo()

        if job is not None:
            procesar_trabajo(job)

            if una_vez:
                break
        else:
            if una_vez:
                print("No hay trabajos pendientes.")
                break

            time.sleep(2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--once",
        action="store_true",
        help="Procesar un solo trabajo y finalizar"
    )

    args = parser.parse_args()

    ejecutar_worker(una_vez=args.once)
