from flask import Flask, request, jsonify
import sqlite3
import json

from database import get_connection, initialize_database
from control_socket import enviar_notificacion

app = Flask(__name__)

# Verificar funcionamiento de la API
@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        "status": "ok",
        "service": "Worker Daemon API"
    }), 200


# Registrar un nuevo trabajo
@app.route('/jobs', methods=['POST'])
def create_job():

    data = request.get_json(silent=True)

    if not data:
        return jsonify({"error": "JSON requerido"}), 400

    job_type = data.get('job_type')
    payload = data.get('payload')
    priority = data.get('priority', 1)

    if not job_type or payload is None:
        return jsonify({
            "error": "job_type y payload son obligatorios"
        }), 400

    if not isinstance(priority, int) or isinstance(priority, bool):
        return jsonify({"error": "priority debe ser un entero"}), 400

    try:
        conn = get_connection()

        cursor = conn.execute("""
            INSERT INTO jobs (job_type, payload, priority)
            VALUES (?, ?, ?)
        """, (job_type, json.dumps(payload), priority))

        job_id = cursor.lastrowid

        conn.commit()
        conn.close()

        try:
            notificacion = enviar_notificacion({
                "event": "job_created",
                "job_id": job_id
            })

            notification_status = notificacion.get(
                "status", "unknown"
            )

        except OSError:
            notification_status = "unavailable"

        return jsonify({
            "message": "Trabajo registrado correctamente",
            "job_id": job_id,
            "status": "pending",
            "notification": notification_status
        }), 201

    except sqlite3.Error as error:
        return jsonify({"error": str(error)}), 500


# Consultar todos los trabajos
@app.route('/jobs', methods=['GET'])
def get_jobs():

    conn = get_connection()

    jobs = conn.execute("""
        SELECT * FROM jobs ORDER BY priority DESC, id ASC
    """).fetchall()

    conn.close()

    return jsonify([dict(job) for job in jobs]), 200


# Consultar un trabajo por ID
@app.route('/jobs/<int:job_id>', methods=['GET'])
def get_job(job_id):

    conn = get_connection()

    job = conn.execute(
        "SELECT * FROM jobs WHERE id = ?",
        (job_id,)
    ).fetchone()

    conn.close()

    if job is None:
        return jsonify({"error": "Trabajo no encontrado"}), 404

    return jsonify(dict(job)), 200


# Eliminar un trabajo pendiente
@app.route('/jobs/<int:job_id>', methods=['DELETE'])
def delete_job(job_id):

    conn = get_connection()

    cursor = conn.execute("""
        DELETE FROM jobs
        WHERE id = ? AND status = 'pending'
    """, (job_id,))

    conn.commit()
    deleted = cursor.rowcount

    conn.close()

    if deleted == 0:
        return jsonify({
            "error": "Trabajo no encontrado o no está pendiente"
        }), 404

    return jsonify({
        "message": "Trabajo eliminado correctamente"
    }), 200


if __name__ == '__main__':

    initialize_database()

    app.run(
        host='0.0.0.0',
        port=8085,
        debug=False
    )
