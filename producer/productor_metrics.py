# TODO: Implement Kafka Producer
import os
import json
import uuid
import time
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer

# 1. Cargar configuración desde .env
load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC = "system-metrics-topic"

# 2. Configuración de servidores a simular
SERVERS = ['web01', 'web02', 'db01', 'app01', 'cache01']

def generate_metrics(server_id):
    """Genera un diccionario con métricas simuladas."""
    return {
        "message_uuid": str(uuid.uuid4()),
        "server_id": server_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "cpu_percent": round(random.uniform(5.0, 95.0), 2),
            "memory_percent": round(random.uniform(20.0, 80.0), 2),
            "disk_io_mbps": round(random.uniform(0.1, 500.0), 2),
            "network_mbps": round(random.uniform(1.0, 1000.0), 2),
            "error_count": random.choices([0, 1, 2], weights=[90, 8, 2])[0]
        }
    }

def main():
    # 3. Inicializar el Productor con Serialización JSON
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all' # Garantiza que el broker recibió el mensaje
        )
        print(f" Productor iniciado. Enviando métricas a {KAFKA_BROKER}...")

        while True:
            for server in SERVERS:
                payload = generate_metrics(server)
                
                # Enviar mensaje
                producer.send(TOPIC, value=payload)
                print(f" [SENT] {server} - UUID: {payload['message_uuid']}")
            
            # Flush para asegurar que se envíen los mensajes del lote
            producer.flush()
            time.sleep(5) # Esperar 5 segundos antes de la siguiente ráfaga

    except KeyboardInterrupt:
        print("\n Productor detenido por el usuario.")
    except Exception as e:
        print(f" Error en el productor: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()