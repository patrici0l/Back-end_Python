import psycopg2
from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
import requests
from datetime import datetime, timedelta

app = FastAPI()

# --- Configuración de Base de Datos ---
DB_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "database": "plataforma_programadores",
    "user": "postgres",
    "password": "2004"
}

def revisar_y_notificar():
    print(f"[{datetime.now()}] Revisando asesorías próximas...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Margen de 30 minutos desde ahora
        ahora = datetime.now()
        margen_proximo = ahora + timedelta(minutes=30)
        
        # SQL ajustado a tus nombres: email_solicitante, nombre_solicitante
        # Unimos fecha + hora para comparar con el tiempo actual
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
            
            # Limpiamos el nombre para la URL (sin espacios)
            nombre_url = nombre.replace(" ", "_")
            
            # Llamada a tu Jakarta EE en WildFly para generar el link
            # Usamos el puerto 8080 que ya tienes funcionando
            url_jakarta = f"http://localhost:8080/whatsapp-api-1.0/api/whatsapp/link?telefono=593999999999&mensaje=Hola_{nombre_url}_tu_asesoria_esta_proxima"
            
            try:
                response = requests.get(url_jakarta)
                if response.status_code == 200:
                    print(f"    Link generado con éxito: {response.json()['link']}")
            except Exception as e:
                print(f"    Error al llamar a Jakarta: {e}")

            # Marcamos como enviado en la DB para que no se repita
            cur.execute("UPDATE asesorias SET recordatorio_enviado = TRUE WHERE id = %s", (id_as,))
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error en el scheduler: {e}")
    finally:
        if conn:
            conn.close()

# --- Configuración del Scheduler ---
scheduler = BackgroundScheduler()
# Se ejecuta cada 1 minuto
scheduler.add_job(revisar_y_notificar, 'interval', minutes=1)
scheduler.start()

@app.get("/")
def home():
    return {
        "mensaje": "Servicio de Notificaciones Python (FastAPI) Activo",
        "proxima_ejecucion": str(datetime.now() + timedelta(minutes=1)),
        "db_conectada": DB_CONFIG['database']
    }

# Para ejecutar usa: uvicorn main:app --reload