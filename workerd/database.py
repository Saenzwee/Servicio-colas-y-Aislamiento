
import sqlite3
import os

DATABASE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "workerd.db"
)

def get_connection():
    conn = sqlite3.connect(
        DATABASE_PATH,
        timeout=30
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn

def initialize_database():
    conn = get_connection()

    try:
        conn.execute("PRAGMA journal_mode = WAL")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                priority INTEGER DEFAULT 1,
                status TEXT DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                started_at DATETIME,
                completed_at DATETIME,
                error_message TEXT
            )
        """)

        conn.commit()

    finally:
        conn.close()

if __name__ == "__main__":
    initialize_database()
    print("Base de datos inicializada correctamente.")

