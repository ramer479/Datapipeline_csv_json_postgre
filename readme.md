Steps : 

1. Install dockerfile and make sure docker is running 
2. Understand the use case end to end 
3. Algorithm and Flow chart 
3. Prepare conf file 
4. Read data from conf file 
5. Create spark data frame 
6. Data Quality checks 
7. Data cleansing 
8. Post gre SQL database 
10. insert data into post gre SQL 
11. Py tests 
12. Analysis 


In this case, you're given two sets of data from different games ("Wild Wild Chords" and "HarmonicaBots") with different formats (JSON and CSV). You're tasked to merge these data sets and store them in a single data warehouse. 

An overall approach for this task could be:

1. **Extract the Data**: Use Spark's capabilities to read JSON and CSV files. You can create a function that takes the game name and date as arguments, then uses these parameters to construct the file path and load the data.

2. **Transform the Data**: Since the two data sets have different structures, you'll need to transform them into a uniform format. This might involve selecting common fields, renaming columns, handling missing values, and converting data types. You can use Spark's DataFrame transformations for these tasks.

3. **Create a Staging Table**: Load the transformed data into a staging table. In this case, since you're using Spark, the staging table can be a DataFrame. You can perform further transformations here if needed.

4. **Identify New/Updated Records**: Compare the staging table with the target table in the data warehouse to identify new and updated records. You can use Spark's DataFrame operations like `except` for this.

5. **Load the Data**: Load the new and updated records from the staging table into the data warehouse. Since a database that supports SQL queries is preferred, you might use something like PostgreSQL. You can use Spark's JDBC connector to write the DataFrame into the database.

6. **Repeat the Process for Each Game**: Run the above process for each game. This can be done in a loop, or you could use a workflow management tool like Apache Airflow to orchestrate the ETL jobs.

7. **Testing**: Write tests to ensure the correctness of your ETL pipeline. This could involve checking the number of records processed, ensuring no duplicates in the final table, etc.

8. **Answer the Questions**: Once the data is in the data warehouse, you can use SQL queries to answer the questions about gender ratio, youngest and oldest players, etc.

Remember, your pipeline should be idempotent, meaning that you can run it multiple times without changing the result beyond the initial application.

For incremental loads, you could consider adding a timestamp column to the data. This way, you can always load data that was added after the latest timestamp in your data warehouse.

Finally, for the question about scaling the data pipeline, consider approaches such as increasing the resources of your Spark cluster, partitioning the data, using a faster storage format (like Parquet), and implementing a streaming data pipeline if the data can be processed in real-time.









# Ovecell Data Pipeline

This project is aimed at creating a data pipeline to process and load data from two sources, namely "wwc" and "hb", into a database. The pipeline reads data from different formats (JSON and CSV), processes it, and writes the processed data into a PostgreSQL database. 

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Python 3.7+
- PySpark 3.0+
- PostgreSQL

### Installing

1. Clone the repository.

```bash
git clone https://github.com/your-repo/ovecell_data_pipeline.git
```

2. Change directory into the cloned repository.

```bash
cd ovecell_data_pipeline
```

3. Install the required Python dependencies.

```bash
pip install -r requirements.txt
```

### Configuration

The pipeline configuration is done through a YAML file, `config.yaml`. This file contains various settings related to the data sources, database connection, and the output. 

## Running the Pipeline

To execute the pipeline, use the following command:

```bash
python main.py --config_path=<path_to_config>
```

Replace `<path_to_config>` with the path to your configuration file.

## Testing

Unit tests are provided to verify the functionality of the pipeline. You can execute these tests using pytest:

```bash
pytest tests/
```

## Structure of the Pipeline

The pipeline consists of several stages:

1. **Configuration reading**: The pipeline reads configuration parameters from a YAML file.

2. **Data ingestion**: Data is read from JSON and CSV files located at paths specified in the configuration.

3. **Data processing**: The pipeline pre-processes the ingested data, transforming and cleaning it as necessary.

4. **Dimension and Fact table creation**: After processing, the data is transformed into a set of dimension and fact tables.

5. **Loading data into the database**: The dimension and fact tables are then loaded into a PostgreSQL database.


## Authors

* **Raghuram Sistla**
