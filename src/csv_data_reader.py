import os
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, BooleanType, TimestampType

class CsvReader:
    """
    Class for reading CSV files using PySpark.
    """
    def __init__(self, spark, csv_base_dir, csv_schema_config, bad_records_path):
        """
        Constructor for the CsvReader class.

        Parameters:
        spark: SparkSession object
        csv_base_dir: The base directory where the CSV files are located
        csv_schema_config: The schema configuration for the CSV files
        bad_records_path: The path where bad records will be stored
        """
        self.spark = spark
        self.csv_base_dir = csv_base_dir
        self.csv_schema_config = csv_schema_config
        self.bad_records_path = bad_records_path

    def get_files(self):
        """
        Get all the CSV files located in the base directory and its subdirectories.

        Returns:
        A list of CSV file paths
        """
        csv_files = []
        for root, dirs, files in os.walk(self.csv_base_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))
        return csv_files

    def get_schema(self):
        """
        Get the schema for the CSV files based on the schema configuration.

        Returns:
        A StructType object representing the schema
        """
        fields = []
        for field_config in self.csv_schema_config:
            fields.append(self.get_field(field_config))
        return StructType(fields)

    def get_field(self, field_config):
        """
        Get a StructField object based on the field configuration.

        Parameters:
        field_config: A dictionary representing the configuration for a field

        Returns:
        A StructField object
        """
        name = field_config['name']
        data_type = self.get_data_type(field_config['type'])
        if field_config['type'] == 'struct':
            sub_fields = [self.get_field(sub_field) for sub_field in field_config['fields']]
            data_type = StructType(sub_fields)
        return StructField(name, data_type)

    @staticmethod
    def get_data_type(data_type_str):
        """
        Get a DataType object based on the type string.

        Parameters:
        data_type_str: A string representing the type

        Returns:
        A DataType object
        """
        if data_type_str == 'string':
            return StringType()
        elif data_type_str == 'integer':
            return IntegerType()
        elif data_type_str == 'double':
            return DoubleType()
        elif data_type_str == 'boolean':
            return BooleanType()
        elif data_type_str == 'timestamp':
            return TimestampType()

    def read_data(self):
        """
        Read the CSV files and return a DataFrame.

        Returns:
        A DataFrame representing the data in the CSV files
        """
        schema = self.get_schema()
        csv_files = self.get_files()
        df = self.spark.read.option("header", True).option("badRecordsPath", self.bad_records_path).schema(schema).csv(csv_files)
        return df
