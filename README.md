# 🚀 Pipeline de Monitorización: Kafka + MongoDB Atlas

Este proyecto implementa un sistema de procesamiento de datos en tiempo real para la monitorización de infraestructura. El sistema simula métricas de rendimiento de servidores, las distribuye mediante **Apache Kafka** y las procesa de forma dual para su almacenamiento y análisis en **MongoDB Atlas**.

---

## 🏗️ Arquitectura del Sistema

El flujo de datos se divide en tres componentes independientes para garantizar la escalabilidad y cumplir con los requisitos de procesamiento:

| Componente | Responsabilidad | Script |
| :--- | :--- | :--- |
| **Productor** | Simula métricas de 5 servidores (CPU, Mem, Red, Errores) y las envía a Kafka. | `productor_metrics.py` |
| **Consumidor RAW** | Escucha Kafka y guarda cada métrica individual en MongoDB para histórico. | `consumidor_metrics.py` |
| **Consumidor KPIs** | Procesa ventanas de 20 mensajes para calcular promedios y rendimiento. | `consumidor_kpis.py` |

---

## 🛠️ Stack Tecnológico

* **Lenguaje:** ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
* **Mensajería:** ![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)
* **Base de Datos:** ![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white)
* **Infraestructura:** ![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)

---

## 🛠️ Preparación del Entorno

Para configurar el proyecto por primera vez, ejecuta los siguientes comandos en tu terminal:

```bash
# 1. Levantar la infraestructura de Kafka con Docker
docker-compose up -d

# 2. Instalar las librerías de Python necesarias
pip install -r requirements.txt
Variables de Entorno (.env)
Asegúrate de tener configurado tu archivo .env con tus credenciales de MongoDB Atlas:

Fragmento de código
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=system-metrics-topic
MONGO_URI=mongodb+srv://iabd11:TuPasswordReal@cluster0.2xlq1lj.mongodb.net/monitoring_db
MONGO_DB_NAME=monitoring_db
📈 Ejecución del Proyecto
Para observar el funcionamiento completo del pipeline, abre tres terminales independientes y ejecuta los scripts en este orden estricto:

1️⃣ Terminal 1: Consumidor de Histórico (RAW)
Este componente se encarga de persistir cada métrica individual en la colección system_metrics_raw.

Bash
python consumer/consumidor_metrics.py
2️⃣ Terminal 2: Consumidor de KPIs (Análisis)
Este componente procesa los datos en tiempo real para generar estadísticas agregadas.

Bash
python consumer/consumidor_kpis.py
3️⃣ Terminal 3: Productor de Datos (Simulador)
Una vez los consumidores estén listos, inicia la generación de métricas simuladas.

Bash
python productor_metrics.py
📊 Procesamiento de Datos (KPIs)
El script consumidor_kpis.py implementa una lógica de ventana tumbling de 20 mensajes. Esto significa que cada vez que se reciben 20 métricas de Kafka, el sistema calcula automáticamente:

Promedios: Rendimiento de CPU, Memoria, I/O de Disco y Red.

Alertas: Suma total de errores detectados en la ráfaga de mensajes.

Rendimiento del Pipeline: Cálculo de velocidad de procesamiento en mensajes por segundo (msg/sec).

[!IMPORTANT]
Todos los resultados del análisis se guardan automáticamente en la colección de MongoDB: system_metrics_kpis.