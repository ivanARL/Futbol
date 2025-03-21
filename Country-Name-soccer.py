from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import json
import os

if __name__ == "__main__":
    # Iniciar Spark
    spark = SparkSession \
        .builder \
        .appName("Country Name Analysis") \
        .getOrCreate()

    print("Cargando former_names.csv ... ")
    path_countries = "former_names.csv"
    
    # Verificar si el archivo existe
    try:
        df_countries = spark.read.csv(path_countries, header=True, inferSchema=True)
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        spark.stop()
        exit(1)
    
    # Ver esquema del DataFrame
    df_countries.printSchema()
    df_countries.show(5)

    # Convertir columnas de fecha a tipo Date
    df_countries = df_countries.withColumn("start_date", to_date(df_countries["start_date"], "yyyy-MM-dd"))
    df_countries = df_countries.withColumn("end_date", to_date(df_countries["end_date"], "yyyy-MM-dd"))
    df_countries.createOrReplaceTempView("countries")
    
    # Describir el DataFrame
    spark.sql('DESCRIBE countries').show(20)

    # Consulta 1: Mostrar países donde el nombre antiguo es 'Dahomey'
    query = """SELECT current, former, start_date, end_date 
               FROM countries 
               WHERE former = 'Dahomey' 
               ORDER BY start_date"""
    df_countries_dahomey = spark.sql(query)
    df_countries_dahomey.show(20)

    # Consulta 2: Seleccionar los países que cambiaron de nombre entre 1950 y 1970
    query = """SELECT current, former, start_date, end_date 
               FROM countries 
               WHERE start_date BETWEEN '1950-01-01' AND '1970-12-31' 
               ORDER BY start_date"""
    df_countries_1950_1970 = spark.sql(query)
    df_countries_1950_1970.show(20)

    # Crear el directorio results si no existe
    os.makedirs('results', exist_ok=True)

    # Guardar los resultados como JSON
    results = df_countries_1950_1970.toJSON().collect()
    with open('results/countries_1950_1970.json', 'w') as file:
        json.dump(results, file)

    # Consulta 3: Contar cuántos países cambiaron de nombre en cada década
    query = """SELECT 
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
    df_countries_decade_count = spark.sql(query)
    df_countries_decade_count.show()

    # Detener Spark al finalizar
    spark.stop()

