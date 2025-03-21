from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import json
import os

# Crear el directorio 'results' si no existe
os.makedirs("results", exist_ok=True)

# Iniciar Spark
spark = SparkSession.builder.appName("Country Name Analysis").getOrCreate()

def load_csv(path):
    """Función para cargar el archivo CSV y manejar errores"""
    try:
        df = spark.read.csv(path, header=True, inferSchema=True)
        return df
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        spark.stop()
        exit(1)

if __name__ == "__main__":
    print("Cargando former_names.csv ... ")
    path_countries = "former_names.csv"
    
    # Verificar si el archivo existe
    if not os.path.exists(path_countries):
        print(f"El archivo {path_countries} no se encuentra en la ruta especificada.")
        spark.stop()
        exit(1)

    # Cargar el CSV
    df_countries = load_csv(path_countries)
    
    # Verificar esquema y mostrar los primeros registros
    df_countries.printSchema()
    df_countries.show(5)

    # Asegurarse de que las columnas 'start_date' y 'end_date' existen
    if "start_date" not in df_countries.columns or "end_date" not in df_countries.columns:
        print("Las columnas 'start_date' y/o 'end_date' no están presentes en el archivo CSV.")
        spark.stop()
        exit(1)

    # Convertir columnas de fecha a tipo Date, manejar posibles errores
    df_countries = df_countries.withColumn("start_date", to_date(df_countries["start_date"], "yyyy-MM-dd"))
    df_countries = df_countries.withColumn("end_date", to_date(df_countries["end_date"], "yyyy-MM-dd"))

    # Verificar si la conversión fue exitosa
    df_countries.show(5)

    # Crear vista temporal para consultas SQL
    df_countries.createOrReplaceTempView("countries")

    # Describir el DataFrame
    spark.sql('DESCRIBE countries').show(20)

    # Consulta 1: Mostrar todos los países sin filtros de fechas
    query_all = """SELECT current, former, start_date, end_date 
                   FROM countries
                   ORDER BY start_date"""
    df_countries_all = spark.sql(query_all)
    df_countries_all.show()  # Muestra todos los registros ordenados por start_date

    # Guardar todos los resultados como JSON
    results_all = df_countries_all.toJSON().collect()
    with open('results/countries_all.json', 'w') as file:
        json.dump(results_all, file)

    # Consulta 2: Contar cuántos países cambiaron de nombre en cada década
    query_decade_count = """SELECT 
                               CASE
                                   WHEN start_date BETWEEN '1900-01-01' AND '1910-12-31' THEN '1900-1910'
                                   WHEN start_date BETWEEN '1910-01-01' AND '1920-12-31' THEN '1910-1920'
                                   WHEN start_date BETWEEN '1920-01-01' AND '1930-12-31' THEN '1920-1930'
                                   WHEN start_date BETWEEN '1930-01-01' AND '1940-12-31' THEN '1930-1940'
                                   WHEN start_date BETWEEN '1940-01-01' AND '1950-12-31' THEN '1940-1950'
                                   WHEN start_date BETWEEN '1950-01-01' AND '1960-12-31' THEN '1950-1960'
                                   WHEN start_date BETWEEN '1960-01-01' AND '1970-12-31' THEN '1960-1970'
                                   WHEN start_date BETWEEN '1970-01-01' AND '1980-12-31' THEN '1970-1980'
                                   ELSE 'Other'
                               END AS decade, COUNT(*) AS country_count
                           FROM countries
                           GROUP BY decade
                           ORDER BY decade"""
    df_countries_decade_count = spark.sql(query_decade_count)
    df_countries_decade_count.show()

    # Detener Spark al finalizar
    spark.stop()
