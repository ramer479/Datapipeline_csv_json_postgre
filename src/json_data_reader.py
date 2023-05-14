import os
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, BooleanType, TimestampType

class JsonDataReader:
    """
    Class for reading JSON files using PySpark.
    """
    def __init__(self, spark, base_dir, schema_config, bad_records_path):
        """
        Constructor for the JsonDataReader class.

        Parameters:
        spark: SparkSession object
        base_dir: The base directory where the JSON files are located
        schema_config: The schema configuration for the JSON files
        bad_records_path: The path where bad records will be stored
        """
        self.spark = spark
        self.base_dir = base_dir
        self.schema_config = schema_config
        self.bad_records_path = bad_records_path

    def get_files(self):
        """
        Get all the JSON files located in the base directory and its subdirectories.

        Returns:
        A list of JSON file paths
        """
        json_files = []
        for root, dirs, files in os.walk(self.base_dir):
            for file in files:
                if file.endswith(".json"):
                    json_files.append(os.path.join(root, file))
        print(f"Found {len(json_files)} JSON files.")
        return json_files

    def get_schema(self):
        """
        Get the schema for the JSON files based on the schema configuration.

        Returns:
        A StructType object representing the schema
        """
        fields = []
        for field_config in self.schema_config:
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
        Read the JSON files and return a DataFrame.

        Returns:
        A DataFrame representing the data in the JSON files
        """
        schema = self.get_schema()
        json_files = self.get_files()
        df = self.spark.read.option("badRecordsPath", self.bad_records_path).schema(schema).json(json_files)
        print("Successfully read data from JSON files.")
        return df
