from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import when
from pyspark.sql.functions import lower, trim
from pyspark.sql.functions import col
import psycopg2
import configparser
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, BooleanType, TimestampType


def get_spark_session(app_name):
    session = SparkSession \
        .builder \
        .master("local") \
        .appName(app_name) \
        .config("spark.driver.extraClassPath", "C:\\Users\\Raghuram\\jars\\postgresql-42.6.0.jar") \
        .config("spark.jars", "C:\\Users\\Raghuram\\jars\\postgresql-42.6.0.jar") \
        .getOrCreate()
    return session


def get_db_config(config_file='db_creds.config'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DATABASE']


db_config = get_db_config()


def connect_to_db():
    conn = psycopg2.connect(
        dbname=db_config['database'],
        user=db_config['user'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port']
    )
    return conn


def write_data_to_database(df, mode, table_name):
    dbname = db_config['database']
    user = db_config['user']
    password = db_config['password']
    host = db_config['host']
    port = db_config['port']
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"

    properties = {"user": user, "password": password, "driver": "org.postgresql.Driver"}

    df.write.jdbc(url=jdbc_url, table=table_name, mode=mode, properties=properties)


def merge_tables(connection, main_table, stg_table, unique_key):
    cursor = connection.cursor()

    # First, dynamically generate the SQL for the columns
    qry_tbl =  f"{main_table}".split(".")[1]
    query_tbl = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{qry_tbl}' " \
                f"order by ordinal_position"
    cursor.execute(query_tbl)
    columns = [row[0] for row in cursor.fetchall()]

    # Then, generate the SQL for the merge (upsert)
    update_columns = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns)
    print(update_columns)
    query_merge = f"""
        INSERT INTO {main_table}
        SELECT * FROM {stg_table}
        ON CONFLICT ({unique_key})
        DO UPDATE SET {update_columns}
    """
    print(query_merge)
    cursor.execute(query_merge)
    connection.commit()


def pre_process_data(spark, raw_df):
    # Step1: Clean the Junk values [Non-ASCII] or data in foreign languages in major columns
    stage1_filtered_df = raw_df.filter(col("name.first").rlike("^([A-Z]|[0-9]|[a-z])+$"))
    stage1_filtered_df = stage1_filtered_df.filter(col("name.last").rlike("^([A-Z]|[0-9]|[a-z])+$"))

    # First flatten the structure
    flat_df = stage1_filtered_df.select(col("name.title").alias("title"),
                                        col("name.first").alias("first"),
                                        col("name.last").alias("last"),"*")

    # Perform the operation to normalize the title
    stage2_filtered_df = flat_df.withColumn(
        "title",
        when(lower(trim(col("title"))) == "monsieur", "mr")
            .when(lower(trim(col("title"))) == "ms", "miss")
            .when(lower(trim(col("title"))) == "madame", "mrs")
            .when(lower(trim(col("title"))) == "mademoiselle", "miss")
            .otherwise(trim(col("title")))
    )

    # Reconstruct the structure
    final_df = stage2_filtered_df.select("*")

    return final_df
