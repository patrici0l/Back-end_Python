import psycopg2
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import requests
from datetime import datetime, timedelta

app = FastAPI()

#  CORS para Angular
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Configuración de Base de Datos ---
DB_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "database": "plataforma_programadores",
    "user": "postgres",
    "password": "2004"
}

#  Estado del scheduler (para Angular)
SCHED_STATUS = {
    "last_run": None,
    "jobs_ok": 0,
    "jobs_fail": 0,
    "last_error": None
}

def revisar_y_notificar():
    global SCHED_STATUS
    conn = None

    # registrar ejecución
    SCHED_STATUS["last_run"] = datetime.now().isoformat(timespec="seconds")
    SCHED_STATUS["last_error"] = None

    print(f"[{datetime.now()}] Revisando asesorías próximas...")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        ahora = datetime.now()
        margen_proximo = ahora + timedelta(minutes=30)

        query = """
            SELECT id, email_solicitante, nombre_solicitante, comentario 
            FROM asesorias 
            WHERE estado = 'aprobada' 
              AND recordatorio_enviado = FALSE 
              AND (fecha + hora) <= %s
        """
        cur.execute(query, (margen_proximo,))
        asesorias = cur.fetchall()

        for aseso in asesorias:
            id_as, email, nombre, msg = aseso
            print(f">>> Notificando a: {nombre} ({email})")

            nombre_url = (nombre or "").replace(" ", "_")

            #  llamada a Jakarta para generar link
            url_jakarta = (
                "http://localhost:8080/whatsapp-api-1.0/api/whatsapp/link"
                f"?telefono=593999999999&mensaje=Hola_{nombre_url}_tu_asesoria_esta_proxima"
            )

            try:
                response = requests.get(url_jakarta, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    print(f"    Link generado con éxito: {data.get('link')}")
                else:
                    print(f"    Jakarta respondió {response.status_code}: {response.text[:120]}")
            except Exception as e:
                print(f"    Error al llamar a Jakarta: {e}")

            #  marcar como enviado
            cur.execute(
                "UPDATE asesorias SET recordatorio_enviado = TRUE WHERE id = %s",
                (id_as,)
            )

        conn.commit()
        cur.close()

        #  ejecución OK
        SCHED_STATUS["jobs_ok"] += 1

    except Exception as e:
        #  ejecución FALLIDA
        SCHED_STATUS["jobs_fail"] += 1
        SCHED_STATUS["last_error"] = str(e)
        print(f"Error en el scheduler: {e}")

    finally:
        if conn:
            conn.close()

# --- Configuración del Scheduler ---
scheduler = BackgroundScheduler()
scheduler.add_job(revisar_y_notificar, 'interval', minutes=1)
scheduler.start()

#  Endpoint raíz
@app.get("/")
def home():
    return {
        "mensaje": "Servicio de Notificaciones Python (FastAPI) Activo",
        "proxima_ejecucion": str(datetime.now() + timedelta(minutes=1)),
        "db_conectada": DB_CONFIG['database']
    }

#  Health (para Angular)
@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": datetime.now().isoformat(timespec="seconds")
    }

# Scheduler Status (para Angular)
@app.get("/scheduler/status")
def scheduler_status():
    return SCHED_STATUS

# Ejecutar: uvicorn main:app --reload --port 8000
