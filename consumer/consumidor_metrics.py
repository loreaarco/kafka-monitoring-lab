#Consumidor de Datos Brutos (Raw)
import os
import json
import sys
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

def main():
    # 1. Conexión y Validación de MongoDB
    if not MONGO_URI:
        print("Error: La variable MONGO_URI no está definida en el archivo .env")
        sys.exit(1)

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Forzar una prueba de conexión (Ping)
        client.admin.command('ping')
        print("Conexión exitosa a MongoDB Atlas.")
    except OperationFailure:
        print("Error: Autenticación fallida en MongoDB. Revisa usuario/password en el .env")
        sys.exit(1)
    except Exception as e:
        print(f"Error al conectar a MongoDB: {e}")
        sys.exit(1)

    db = client[MONGO_DB_NAME]
    collection = db['system_metrics_raw']

    # 2. Configuración del Consumidor de Kafka
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id='group-raw-storage',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Consumidor Kafka conectado a {KAFKA_BROKER}")
        print(f"Escuchando en el tópico: '{TOPIC}'...")
    except Exception as e:
        print(f"Error al conectar con Kafka: {e}")
        sys.exit(1)

    # 3. Bucle de Consumo
    print("\n--- Consumidor RAW iniciado. Presiona Ctrl+C para salir ---\n")
    
    try:
        for message in consumer:
            data = message.value
            
            try:
                # Insertar en MongoDB
                result = collection.insert_one(data)
                
                # Feedback visual
                server_id = data.get('server_id', 'unknown')
                uuid = data.get('message_uuid', 'N/A')
                print(f" Guardado Raw: Server: {server_id} | UUID: {uuid} | ID_Mongo: {result.inserted_id}")
                
            except Exception as e:
                print(f"Error al insertar documento: {e}")

    except KeyboardInterrupt:
        print("\nStopping Consumidor RAW...")
    finally:
        print("Cerrando conexiones...")
        client.close()
        consumer.close()

if __name__ == "__main__":
    main()