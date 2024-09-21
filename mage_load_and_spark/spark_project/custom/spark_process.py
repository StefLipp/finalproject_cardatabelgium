import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import pandas as pd
from pyspark.sql import types
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import expr, round

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    #imports

    spark = kwargs.get('spark')
    spark.stop()
    credentials_location = '/home/src/mage_planner_key.json'
    
    #define params
    # Initialize Spark session with required configurations
    
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[*]") \
        .config("spark.jars", "/home/src/lib/gcs-connector-hadoop3-latest.jar ,/home/src/lib/spark-bigquery-with-dependencies_2.12-0.41.0.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
        .getOrCreate()


    # Ensure correct project and temporary bucket configuration
    spark.conf.set("parentProject", "carprojectbelgium")
    spark.conf.set("temporaryGcsBucket", "tempdata-stefl")
    spark.conf.set("credentialsFile", credentials_location)


    input_car = 'gs://carproject_datalake/raw/statbelcar_data.csv'
    input_city = 'gs://carproject_datalake/raw/wp_municipality_data.csv'
    output_car = 'gs://carproject_datalake/pq/cardata'
    output_city = 'gs://carproject_datalake/pq/citydata'
    bq_output_car = 'carprojectbelgium.belgium_cardata.spark_cardata2017-2023'
    bq_output_city = 'carprojectbelgium.belgium_cardata.spark_muncipalitydata'

    #reading dfs
    df_car = spark.read \
        .option("header", "true") \
        .csv(input_car).drop("Unnamed: 0")

    df_city = spark.read \
        .option("header", "true") \
        .csv(input_city).drop("Unnamed: 0")


    #define schema
    car_schema=types.StructType([
        types.StructField('Unnamed: 0', types.IntegerType(), True),
        types.StructField('CD_YEAR', types.ShortType(), True), 
        types.StructField('CD_REFNIS', types.IntegerType(), True), 
        types.StructField('TX_MUNTY_DESCR_FR', types.StringType(), True), 
        types.StructField('TX_MUNTY_DESCR_NL', types.StringType(), True), 
        types.StructField('TX_MUNTY_DESCR_DE', types.StringType(), True), 
        types.StructField('TX_MUNTY_DESCR_EN', types.StringType(), True), 
        types.StructField('hh_type', types.StringType(), True), 
        types.StructField('hh_wagens', types.StringType(), True), 
        types.StructField('total_huisH', types.IntegerType(), True), 
        types.StructField('total_wagens', types.IntegerType(), True)])

    city_schema=types.StructType([
        types.StructField('Unnamed: 0', types.IntegerType(), True),
        types.StructField('Rang', types.FloatType(), True), 
        types.StructField('Gemeente', types.StringType(), True),
        types.StructField('Deel- gem. Aantal', types.StringType(), True), 
        types.StructField('1846.0', types.FloatType(), True), 
        types.StructField('1900.0', types.FloatType(), True), 
        types.StructField('1947.0', types.FloatType(), True), 
        types.StructField('2000.0', types.FloatType(), True), 
        types.StructField('2024[3]', types.FloatType(), True), 
        types.StructField('Opp. (km²)[2]', types.FloatType(), True), 
        types.StructField('Inw./ km²', types.FloatType(), True), 
        types.StructField('2021 Welv.- index', types.StringType(), True), 
        types.StructField('Provincie of gewest', types.StringType(), True)])



    #assign schema to dfs and output to parquet
    df_car = spark.read \
            .option("header", "true") \
            .option("mergeSchema", "true") \
            .schema(car_schema) \
            .csv(input_car).drop("Unnamed: 0")
        
    df_car.repartition(4).write.mode('overwrite').parquet(output_car)

    df_city = spark.read \
        .option("header", "true") \
        .option("mergeSchema", "true") \
        .schema(city_schema) \
        .csv(input_city) \
        .drop("Unnamed: 0")

    df_city = df_city \
        .withColumnRenamed("Deel- gem. Aantal", "count_boroughs") \
        .withColumnRenamed("1846.0", "year_1846") \
        .withColumnRenamed("1900.0", "year_1900") \
        .withColumnRenamed("1947.0", "year_1947") \
        .withColumnRenamed("2000.0", "year_2000") \
        .withColumnRenamed("2024[3]", "year_2024") \
        .withColumnRenamed("Opp. (km²)[2]", "area_km2") \
        .withColumnRenamed("Inw./ km²", "pop_per_km2") \
        .withColumnRenamed("2021 Welv.- index", "welv_Index") \
        .withColumnRenamed("Provincie of gewest", "province_or_region")

    df_city = df_city \
                .withColumn("Rang",df_city.Rang.cast(types.IntegerType())) \
                .withColumn("year_1846",df_city.year_1846.cast(types.IntegerType())) \
                .withColumn("year_1900",df_city.year_1900.cast(types.IntegerType())) \
                .withColumn("year_1947",df_city.year_1947.cast(types.IntegerType())) \
                .withColumn("year_2000",df_city.year_2000.cast(types.IntegerType())) \
                .withColumn("year_2024",df_city.year_2024.cast(types.IntegerType()))

    df_city.write.mode('overwrite').parquet(output_city)


    #read temporary pqs to df
    df_car = spark.read.parquet(output_car)
    df_city = spark.read.parquet(output_city)


    #clean data
    df_city_cleaned = df_city.withColumn("count_boroughs", regexp_replace("count_boroughs", "\\*", "")) \
                            .withColumn("Gemeente", regexp_replace("Gemeente", "\\*", "")) 

    #define datatype boroughs
    df_city_cleaned = df_city_cleaned.withColumn("count_boroughs",df_city_cleaned.count_boroughs.cast(types.IntegerType()))

    #add calculated column
    df_city_cleaned = df_city_cleaned.withColumn(
        'pop_perc_increase_2024v2000',
        round(expr("(year_2024 - year_2000) / year_2000 * 100"), 2)
    )

    #define translation function for household types
    def household_transl(hh):
        if hh == "Ménages d'une personne":
            return "1 person Household"
        elif hh == "Couples avec enfant(s) cohabitant(s)":
            return "Couples with cohabiting children"
        elif hh == "Autres types de ménages":
            return "Other types of households"
        elif hh == "Couples sans enfant cohabitant":
            return "Couples without cohabiting children"
        elif hh == "Familles monoparentales":
            return "Single-parent families"
        else:
            return unidentified

    hhtransl_udf = F.udf(household_transl, returnType=types.StringType())


    #add translated column
    df_car_cleaned = df_car.withColumn('hh_type_EN', hhtransl_udf(df_car.hh_type))

    #define function for car status
    def hascar(carcount):
        if carcount == '1' or carcount == '2' or carcount == ">2":
            return "has_car"
        else:
            return "no_car"

    hascar_udf = F.udf(hascar, returnType=types.StringType())

    #add car status column
    df_car_cleaned = df_car_cleaned.withColumn('car_status', hascar_udf(df_car_cleaned.hh_wagens))

    # Set the correct project ID
    spark.conf.set("parentProject", "carprojectbelgium")
    spark.conf.set("temporaryGcsBucket", "tempdata-stefl")

    #write to bigquery
    df_car_cleaned.write \
        .format('bigquery') \
        .option('table', bq_output_car) \
        .option("temporaryGcsBucket", "tempdata-stefl") \
        .option("credentialsFile", credentials_location) \
        .mode("overwrite") \
        .save()

    df_city_cleaned.write \
        .format('bigquery') \
        .option('table', bq_output_city) \
        .option("temporaryGcsBucket", "tempdata-stefl") \
        .option("credentialsFile", credentials_location) \
        .mode("overwrite") \
        .save()





@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
