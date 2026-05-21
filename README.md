# SDPD2. Práctica final - Grupo 3

Este repositorio contiene el proyecto final de la asignatura **Sistemas Distribuidos de Procesamiento de Datos II**. El objetivo principal es el diseño, desarrollo y despliegue de pipelines de datos distribuidos e inferencia analítica, combinando arquitecturas batch y streaming.


## Desarrollo del Proyecto

### Primera fase: EDA y pipeline de datos en batch con Airflow
La primera fase trata sobre el diseño y desarrollo de un pipeline de datos (ETL) utilizando Apache Airflow, con el objetivo de transformar datos en bruto en información estructurada y distribuirla a través de sistemas de mensajería en tiempo real como es Apache Kafka. 

Además, en esta primera práctica hemos realizado también un Análisis Exploratorio de datos (EDA) del conjunto de datos de restaurantes europeos de TripAdvisor, analizando la estructura del dataset, los valores faltantes, las correlaciones entre variables y la calidad de los datos. 

Finalmente, se implementó un DAG, compuesto por diferentes tareas de extracción, limpieza, transformación y almacenamiento de los datos, permitiendo automatizar todo el flujo de procesamiento. 

### Segunda fase: procesamiento de flujos de datos con Apache Kafka y Spark Streaming
Respecto a la segunda práctica, aborda el procesamiento de flujos de datos estructurados. Para ello, se configuró un sistema de mensajería con Apache Kafka, utilizando un cliente en Python que simula y envía continuamente eventos de compras al topic purchases. Posteriormente, se implementó una aplicación con PySpark para consumir este flujo de información en tiempo real. 

Finalmente, se diseñaron consultas sobre el stream empleando distintos modos de salida de Spark Structured Streaming (append y complete). Esto permitió, por un lado, registrar el histórico completo de los mensajes y, por otro, generar agregaciones dinámicas para contabilizar los productos, volcando automáticamente los resultados finales en ficheros de texto. 

### Tercera fase: pipeline de modelo de ML
Por último, hemos incorporado en esta nueva práctica un diseño funcional de un pipeline que se podría desarrollar para implementar un modelo de ML sobre un flujo de datos (streaming data) proveniente del dataset utilizado.  


## Estructura del Proyecto

El código fuente se organiza en módulos independientes para cada una de las fases desarrolladas durante el curso:

### `practica1/` 

* **`dag_tripadvisor.py`**: Contiene el código fuente principal que implementa el DAG utilizando la TaskFlow API de Airflow.
* **`config.toml`**: Archivo de configuración externa que centraliza las rutas de datos y parámetros de conexión a Kafka, permitiendo modificar el comportamiento sin alterar el código fuente.
* **`tripadvisor_european_restaurants.csv`**: Dataset original en formato CSV utilizado como datos de entrada para el pipeline. El enlace al dataset es el siguiente: https://www.kaggle.com/datasets/stefanoleone992/tripadvisor-european-restaurants/data 
* **`EDA_TripAdvisor_European_restaurants.ipynb`**: Código utilizado para el análisis exploratorio de datos del dataset original.
* **`resultados_limpios.parquet`**: Archivo de salida optimizado en formato Parquet generado tras la ejecución, limpieza y transformación del pipeline.

### `practica2/` 

* **`kafka-producer-confluent.py`**: Cliente productor de Kafka que genera de forma sintética eventos aleatorios de compra y los publica continuamente en el topic `purchases`.
* **`kafka-consumer-confluent.py`**: Cliente consumidor básico basado en Confluent Kafka utilizado para realizar comprobaciones rápidas y validar la correcta llegada de datos al topic.
* **`struct_kafka_consumer_local.py`**: Configura un flujo de entrada asíncrono mediante Spark Structured Streaming para procesar los datos de Kafka desde el origen (`earliest`), exportando agregaciones y volcados de datos.


## Tecnologías Utilizadas
* **Lenguaje Principal:** Python (v3.11+)
* **Orquestación Batch:** Apache Airflow (TaskFlow API)
* **Procesamiento en Streaming:** Apache Spark v4.1.1 (Spark Structured Streaming)
* **Mensajería Distribuida:** Apache Kafka (Confluent Kafka Client)
* **Formatos de Almacenamiento y Configuración:** CSV, Parquet, TOML

## Guía de Ejecución del Proyecto

### Fase 1: Orquestación con Apache Airflow

1. Levantar el servidor de Apache Airflow e iniciar la base de datos.
2. Acceder a la interfaz web a través del navegador en `localhost:8080`.
3. Activar y lanzar el DAG `dag_tripadvisor.py`. 
4. Verificar que el archivo de salida estructurado `resultados_limpios.parquet` se haya generado correctamente en la ruta correspondiente.

### Fase 2: Streaming de Datos

#### 1. Carga de Datos en Apache Kafka
Configuración del entorno virtual e instalación de Kafka utilizando `venv`:

```bash
# Crear el entorno virtual para Kafka
python3 -m venv kafka-env

# Activar el entorno virtual
source kafka-env/bin/activate

# Instalar la librería cliente de Confluent Kafka
pip install confluent-kafka

# Ejecutar el productor para cargar eventos aleatorios en el topic "purchases"
python3 kafka-producer-confluent.py

# Ejecutar el consumidor de consola para verificar la recepción de claves y valores
python3 kafka-consumer-confluent.py
```
#### 2. Procesamiento de datos con Spark Structured Streaming
Configuración del entorno de procesamiento distribuido e instalación de PySpark.

```bash
# Crear el entorno virtual para PySpark
python3 -m venv pyspark-411

# Activar el entorno virtual de Spark
source pyspark-411/bin/activate

# Instalar PySpark (versión 4.1.1)
pip install pyspark==4.1.1

# Ejecutar el consumidor estructurado de Spark conectado al broker local
python3 struct_kafka_consumer_local.py
```
#### 3. Verificación de Resultados
```bash
# Ver el contenido de los fragmentos de texto generados en la Consulta 1 (Modo append)
cat salida1.txt

# Ver el estado consolidado de los conteos de productos en la Consulta 2 (Modo complete)
cat salida2.txt
```

## Autores
* Lucía Arnaldo Cuevas
* Jessica García Blanco
* Aitana García Herranz
* Carmen Liberal Jiménez

