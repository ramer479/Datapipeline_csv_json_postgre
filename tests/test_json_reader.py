import pytest
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.json_data_reader import JsonDataReader

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local") \
        .appName("JsonDataReaderTest") \
        .getOrCreate()
    yield spark

def test_get_files(tmpdir):
    base_dir = tmpdir.mkdir("json_files")
    base_dir.join("file1.json").write('content')
    base_dir.join("file2.json").write('content')
    base_dir.join("file3.txt").write('content')

    reader = JsonDataReader(None, str(base_dir), None, None)

    json_files = reader.get_files()

    assert len(json_files) == 2
    assert str(base_dir.join("file1.json")) in json_files
    assert str(base_dir.join("file2.json")) in json_files
    assert str(base_dir.join("file3.txt")) not in json_files

def test_get_schema():
    schema_config = [
        {"name": "field1", "type": "string"},
        {"name": "field2", "type": "integer"}
    ]

    reader = JsonDataReader(None, None, schema_config, None)

    schema = reader.get_schema()

    expected_schema = StructType([
        StructField("field1", StringType()),
        StructField("field2", IntegerType())
    ])

    assert schema == expected_schema
