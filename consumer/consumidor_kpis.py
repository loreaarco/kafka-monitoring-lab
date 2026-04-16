import os
import json
import time
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.errors import OperationFailure

# Cargar variables de entorno
load_dotenv()

# Configuración
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'monitoring_db')
TOPIC = "system-metrics-topic"
WINDOW_SIZE = 20

def calculate_kpis(batch):
    """Calcula promedios y sumas de la ventana de mensajes."""
    count = len(batch)
    if count == 0: return None
    
    # Extraer listas de valores con seguridad (.get para evitar KeyError)
    cpus = [m.get('metrics', {}).get('cpu_percent', 0) for m in batch]
    mems = [m.get('metrics', {}).get('memory_percent', 0) for m in batch]
    disks = [m.get('metrics', {}).get('disk_io_mbps', 0) for m in batch]
    nets = [m.get('metrics', {}).get('network_mbps', 0) for m in batch]
    errors = sum([m.get('metrics', {}).get('error_count', 0) for m in batch])
    
    return {
        "timestamp_kpi": datetime.now(timezone.utc).isoformat(),
        "messages_processed": count,
        "averages": {
            "cpu_percent": round(sum(cpus) / count, 2),
            "memory_percent": round(sum(mems) / count, 2),
            "disk_io_mbps": round(sum(disks) / count, 2),
            "network_mbps": round(sum(nets) / count, 2)
        },
        "total_errors": errors
    }

def main():
    # 1. Validación de conexión a MongoDB Atlas
    if not MONGO_URI:
        print("Error: MONGO_URI no encontrado en .env")
        sys.exit(1)

    try:
        # serverSelectionTimeoutMS ayuda a fallar rápido si la IP no está autorizada o el pass es mal
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        print("Conexión exitosa a MongoDB Atlas (KPI Processor)")
    except OperationFailure:
        print("Error de Autenticación: El usuario o la contraseña en MONGO_URI son incorrectos.")
        sys.exit(1)
    except Exception as e:
        print(f"Error conectando a MongoDB: {e}")
        sys.exit(1)

    db = client[MONGO_DB_NAME]
    collection = db['system_metrics_kpis']

    # 2. Configuración del Consumidor de Kafka
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id='group-kpi-processor',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Consumidor KPIs escuchando tópico: '{TOPIC}'...")
    except Exception as e:
        print(f"Error conectando a Kafka: {e}")
        sys.exit(1)

    batch = []
    start_time = time.time()

    print(f"Procesando en ventanas de {WINDOW_SIZE} mensajes...")

    try:
        for message in consumer:
            batch.append(message.value)
            
            # Si alcanzamos el tamaño de la ventana definida
            if len(batch) >= WINDOW_SIZE:
                end_time = time.time()
                duration = end_time - start_time
                
                # --- CORRECCIÓN: Evitar ZeroDivisionError ---
                # Si la duración es 0 (procesamiento instantáneo), usamos un valor mínimo
                safe_duration = duration if duration > 0 else 0.0001
                
                # Calcular KPIs de la lista actual
                kpis = calculate_kpis(batch)
                
                if kpis:
                    # Añadir métricas de rendimiento del consumidor
                    kpis["window_duration_sec"] = round(duration, 4)
                    kpis["processing_rate_msg_sec"] = round(WINDOW_SIZE / safe_duration, 2)

                    try:
                        # Guardar resultado en la colección de KPIs
                        collection.insert_one(kpis)
                        print(f"Window OK: CPU Avg {kpis['averages']['cpu_percent']}% | Velocidad: {kpis['processing_rate_msg_sec']} msg/s")
                    except Exception as e:
                        print(f"Error al insertar en MongoDB: {e}")

                # Reiniciar para la siguiente ventana
                batch = []
                start_time = time.time()
                
    except KeyboardInterrupt:
        print("\nDeteniendo Consumidor KPIs...")
    finally:
        print("Cerrando conexiones...")
        client.close()
        consumer.close()

if __name__ == "__main__":
    main()