import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, BooleanType, TimestampType

from src.csv_data_reader import CsvReader  # replace with actual path to your module

class TestCsvReader:
    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder.appName('TestApp').getOrCreate()
        cls.csv_base_dir = '/path/to/csv/files'  # replace with your path
        cls.csv_schema_config = [
            {'name': 'field1', 'type': 'string'},
            {'name': 'field2', 'type': 'integer'},
            {'name': 'field3', 'type': 'string'}
        ]  # replace with your schema config
        cls.bad_records_path = '/path/to/bad/records'  # replace with your path
        cls.csv_reader = CsvReader(cls.spark, cls.csv_base_dir, cls.csv_schema_config, cls.bad_records_path)

    def test_get_files(self):
        files = self.csv_reader.get_files()
        assert isinstance(files, list)
        assert all(f.endswith('.csv') for f in files)

    def test_get_schema(self):
        schema = self.csv_reader.get_schema()
        assert isinstance(schema, StructType)
        assert len(schema) == len(self.csv_schema_config)

    def test_get_field(self):
        for field_config in self.csv_schema_config:
            if field_config['type'] != 'struct':
                field = self.csv_reader.get_field(field_config)
                assert isinstance(field, StructField)
                assert field.name == field_config['name']

    def test_get_data_type(self):
        for field_config in self.csv_schema_config:
            if field_config['type'] != 'struct':
                data_type = self.csv_reader.get_data_type(field_config['type'])
                assert isinstance(data_type, (StringType, IntegerType, DoubleType, BooleanType, TimestampType))

