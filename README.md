# SDPD2. Práctica final - Grupo 3

Este repositorio contiene el proyecto final de la asignatura **Sistemas Distribuidos de Procesamiento de Datos II**. El objetivo principal es el diseño, desarrollo y despliegue de pipelines de datos distribuidos e inferencia analítica, combinando arquitecturas batch y streaming.


## Estructura del Proyecto

El código fuente se organiza en módulos independientes para cada una de las fases desarrolladas durante el curso:

### `practica1/` — Pipeline de Datos en Batch con Airflow

* **`dag_tripadvisor.py`**: Contiene el código fuente principal que implementa el DAG utilizando la TaskFlow API de Airflow.
* **`config.toml`**: Archivo de configuración externa que centraliza las rutas de datos y parámetros de conexión a Kafka, permitiendo modificar el comportamiento sin alterar el código fuente.
* **`tripadvisor_european_restaurants.csv`**: Dataset original en formato CSV utilizado como datos de entrada para el pipeline.
* **`resultados_limpios.parquet`**: Archivo de salida optimizado en formato Parquet generado tras la ejecución, limpieza y transformación del pipeline.

### `practica2/` — Procesamiento de flujos de datos con Apache Kafka y Spark Streaming

* **`kafka-producer-confluent.py`**: Cliente productor de Kafka que genera de forma sintética eventos aleatorios de compra y los publica continuamente en el topic `purchases`.
* **`kafka-consumer-confluent.py`**: Cliente consumidor básico basado en Confluent Kafka utilizado para realizar comprobaciones rápidas y validar la correcta llegada de datos al topic.
* **`struct_kafka_consumer_local.py`**: Configura un flujo de entrada asíncrono mediante Spark Structured Streaming para procesar los datos de Kafka desde el origen (`earliest`), exportando agregaciones y volcados de datos.



