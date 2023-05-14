import os
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import col, lit
import yaml
import argparse
from utils import *
import pyspark.sql.functions as F
from pyspark.sql.functions import concat, regexp_extract
from pyspark.sql.functions import col
from fact_dimension import *
from json_data_reader import *
from csv_data_reader import *
import logging


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("config_path")
    return parser.parse_args()


def read_config():
    print("Config read")
    with open("config.yaml") as file:
        config = yaml.safe_load(file)
    return config


def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logging.info("Reading config...")
    config = read_config()
    data_pipeline = config['execute_data_pipeline']

    logging.info("Starting Spark session...")
    spark = get_spark_session(app_name="OvecellDatapipeline")
    bad_records_path = config['bad_records_path']
    if data_pipeline == "wwc":

        logging.info("Reading Json data...")
        base_dir = config['wwc_file_path']
        json_schema_cfg = config['json_schema_yml']

        json_reader = JsonDataReader(spark, base_dir, schema_config=json_schema_cfg, bad_records_path=bad_records_path)
        json_dataframe = json_reader.read_data()

        logging.info("Pre-processing Json data...")
        json_process_df = pre_process_data(spark, json_dataframe)
        logging.info("Pre-processed Json data...")
        json_process_df.show(truncate=False)

        logging.info("Creating user dimension with wwc...")
        dim_user = create_user_dimension(json_process_df, "wwc")
        logging.info("Writing wwc data to database...")
        logging.info("Extracting the postgre user table details from conf ..")
        dim_user_config = config['user_dimension']
        schema = dim_user_config.get('schema')
        tgt_table = dim_user_config.get('target_table')
        stg_table = dim_user_config.get('stg_table')

        write_data_to_database(dim_user, "overwrite", f"{schema}.{stg_table}")

        logging.info("Merging tables...")

        pg_conn = connect_to_db()
        merge_tables(pg_conn, f"{schema}.{tgt_table}", f"{schema}.{stg_table}", "user_id")
        cur_df = json_process_df

        ########### Location Dimension ############

        logging.info("Creating Location dimension with wwc...")
        dim_loc = create_location_dimension(json_process_df, "wwc")

        logging.info("Extracting the postgre location table details from conf ..")
        dim_loc_config = config['location_dimension']
        schema = dim_loc_config.get('schema')
        tgt_table = dim_loc_config.get('target_table')
        stg_table = dim_loc_config.get('stg_table')

        print(f"the staging table is {stg_table} and the target table is {tgt_table}")
        logging.info("Writing wwc location data to database...")
        write_data_to_database(dim_loc, "overwrite", f"{schema}.{stg_table}")

        logging.info("Merging tables...")

        pg_conn = connect_to_db()
        merge_tables(pg_conn, f"{schema}.{tgt_table}", f"{schema}.{stg_table}", "location_id")

        logging.info(f"Creating Fact Table with {data_pipeline}...")
        fact_user = create_fact_table(cur_df, dim_user=dim_user, dim_loc=dim_loc, src=data_pipeline)

        logging.info("Extracting the postgre location table details from conf ..")
        fact_config = config['user_fact']
        schema = fact_config.get('schema')
        tgt_table = fact_config.get('target_table')
        stg_table = fact_config.get('stg_table')

        print(f"the staging table is {stg_table} and the target table is {tgt_table}")
        logging.info(f"Writing {data_pipeline} fact data to database...")

        write_data_to_database(fact_user, "overwrite", f"{schema}.{stg_table}")

        logging.info("Merging tables...")

        pg_conn = connect_to_db()
        merge_tables(pg_conn, f"{schema}.{tgt_table}", f"{schema}.{stg_table}", "user_id")


    ######################################################################################################
    elif data_pipeline == "hb":

        logging.info("Reading CSV data...")
        base_dir = config['hb_file_path']
        csv_schema_cfg = config['csv_schema_yml']
        csv_reader = CsvReader(spark, base_dir, csv_schema_config=csv_schema_cfg, bad_records_path=bad_records_path)
        csv_dataframe = csv_reader.read_data()
        logging.info("CSV Dataframe .. ")
        csv_dataframe.show()

        logging.info("Pre-processing Csv data...")
        cur_df = csv_dataframe

        logging.info("Writing to database...")
        logging.info("Creating user dimension with wwc...")
        dim_user = create_user_dimension(csv_dataframe, "hb")
        logging.info("Writing hb data to database...")
        write_data_to_database(dim_user, "overwrite", "ovecell.dim_user_stg")

        logging.info("Merging tables...")
        dim_user_config = config['user_dimension']

        schema = dim_user_config.get('schema')
        tgt_table = dim_user_config.get('target_table')
        stg_table = dim_user_config.get('stg_table')
        pg_conn = connect_to_db()
        merge_tables(pg_conn, f"{schema}.{tgt_table}", f"{schema}.{stg_table}", "user_id")

        ########### Location Dimension ############

        logging.info("Creating Location dimension with hb...")
        dim_loc = create_location_dimension(csv_dataframe, "hb")

        logging.info("Extracting the postgre location table details from conf ..")
        dim_loc_config = config['location_dimension']
        schema = dim_loc_config.get('schema')
        tgt_table = dim_loc_config.get('target_table')
        stg_table = dim_loc_config.get('stg_table')

        print(f"the staging table is {stg_table} and the target table is {tgt_table}")
        logging.info("Writing hb location data to database...")
        write_data_to_database(dim_loc, "overwrite", f"{schema}.{stg_table}")

        logging.info("Merging tables...")

        pg_conn = connect_to_db()
        merge_tables(pg_conn, f"{schema}.{tgt_table}", f"{schema}.{stg_table}", "location_id")

        ############### FACT TABLE ################

        logging.info(f"Creating Fact Table with {data_pipeline}...")
        print("%%%%%%%%%% Cur_df ")
        cur_df.orderBy("email").show()

        print("%%%%%%%%%% dim_user ")
        dim_user.orderBy("email").show()

        fact_user = create_fact_table(cur_df, dim_user=dim_user, dim_loc=dim_loc, src=data_pipeline)

        logging.info("Extracting the postgre location table details from conf ..")
        fact_config = config['user_fact']
        schema = fact_config.get('schema')
        tgt_table = fact_config.get('target_table')
        stg_table = fact_config.get('stg_table')

        print(f"the staging table is {stg_table} and the target table is {tgt_table}")
        logging.info(f"Writing {data_pipeline} fact data to database...")

        write_data_to_database(fact_user, "overwrite", f"{schema}.{stg_table}")

        logging.info("Merging tables...")

        pg_conn = connect_to_db()
        merge_tables(pg_conn, f"{schema}.{tgt_table}", f"{schema}.{stg_table}", "user_id")



if __name__ == "__main__":
    main()
