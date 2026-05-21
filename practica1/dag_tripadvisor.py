from airflow.decorators import dag, task
import pendulum
import pandas as pd
import json

# Intentamos importar tomllib (estándar en Python 3.11+) o tomli como alternativa
try:
    import tomllib
except ImportError:
    import tomli as tomllib

# CARGA DE CONFIGURACIÓN
# Leemos las rutas y parámetros desde el archivo config.toml
CONFIG_PATH = "/home/alumnos/luciiaa/airflow/dags/config.toml"
with open(CONFIG_PATH, "rb") as f:
    config = tomllib.load(f)

# Definimos las variables que descartaremos más adelante en la tercera tarea
variables_a_descartar = [
    "atmosphere", "longitude", "reviews_count_in_default_language",
    "total_reviews_count", "very_good", "latitude", "average",
    "working_shifts_per_week", "poor", "terrible",
    "open_hours_per_week", "open_days_per_week"
]

@dag(
    dag_id='TripAdvisor_European_restaurants',
    schedule=None,
    start_date=pendulum.datetime(2026, 4, 9, tz="UTC"),
    catchup=False,
    tags=['SDPD2', 'ETL', 'TripAdvisor'],
)
def TripAdvisor_European_restaurants_pipeline():

    @task
    def extract_data():
        # El primer paso es la lectura del fichero original
        # Le pasamos la ruta en la cual se encuentra el dataset con el que vamos a trabajar
        path = config['pipeline']['input_csv']
        # Usamos low_memory=False para evitar el DtypeWarning y limitamos filas para estabilidad
        df = pd.read_csv(path, low_memory=False, nrows=50000)
        return df.to_json(orient='records')

    @task
    def analyze_and_partition_data(json_data):
        # En el segundo paso realizamos una análisis de duplicados y de particionado
        df = pd.read_json(json_data)
        # Según el Análisis de datos realizado comprobamos si hay duplicado mediante la variable restaurant_link
        inicial = len(df)
        df = df.drop_duplicates(subset=["restaurant_link"])
        print(f"Eliminados {inicial - len(df)} registros duplicados.")
        # Filtramos por países con mejores ratings (opcional, basado en tu ranking)
        # Aquí podrías particionar si quisieras solo un país específico
        return df.to_json(orient='records')

    @task
    def clean_data(json_data):
        # En el tercer paso realizamos una limpieza de las variables basada en el análisis
        # realizado con la matriz de correlación y la información mutua para las variables categóricas
        df = pd.read_json(json_data)
        # 1) Eliminamos las variables que se mostraban irrelevantes, redundantes y con el elevado número de nulos
        df_clean = df.drop(columns=variables_a_descartar, errors='ignore')
        # 2) Tratamiento de nulos en variables clave (food, service, value)
        # Decidimos filtrar los nulos en las variables críticas de la variable objetivo para poder
        # así asegurar la integridad referencial de los datos que enviamos a la fase de transformación
        df_clean = df_clean.dropna(subset=['food', 'service', 'value', 'avg_rating'])
        return df_clean.to_json(orient='records')

    @task
    def transform_data(json_data):
        # En el cuarto paso realizamos transformaciones finales
        df = pd.read_json(json_data)
        # Hemos pensado crear una métrica combinada en la que nos resume la calidad general del restaurante
        df['quality_score'] = df[['food', 'service', 'value']].mean(axis=1)
        return df.to_json(orient='records')

    @task
    def store_data(json_data):
        # Convertimos en JSON lo que viene de la tarea anterior a un Dataframe
        df = pd.read_json(json_data)
        # Definimos la ruta del archivo donde se guardará (desde el archivo config)
        ruta_archivo = config['pipeline']['output_csv']
        # Guardamos el archivo en la ruta indicada
        df.to_csv(ruta_archivo, index=False)
        return "Se ha procesado con éxito" # Devolvemos un mensaje para confirmar en los logs

    # --- Definición del flujo de trabajo ---
    data_raw = extract_data()
    data_analyzed = analyze_and_partition_data(data_raw)
    data_cleaned = clean_data(data_analyzed)
    data_transformed = transform_data(data_cleaned)
    final_status = store_data(data_transformed)

# Instanciación del pipeline
TripAdvisor_European_restaurants_pipeline()