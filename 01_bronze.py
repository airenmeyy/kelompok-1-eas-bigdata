from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os

# Setup Spark Session with Delta Lake support
builder = SparkSession.builder.appName("Kecamatra-Bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define paths
raw_data_path = "./raw_data"
bronze_path = "./lakehouse/bronze"

# Ensure bronze directory exists
os.makedirs(bronze_path, exist_ok=True)


def sanitize_column_name(name: str) -> str:
    sanitized = name.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    sanitized = ''.join(ch if ch.isalnum() or ch == '_' else '_' for ch in sanitized)
    sanitized = sanitized.strip('_')
    if sanitized == '':
        sanitized = 'column'
    return sanitized


def sanitize_dataframe_columns(df):
    columns = df.columns
    sanitized_columns = []
    seen = {}
    for col in columns:
        safe = sanitize_column_name(col)
        count = seen.get(safe, 0)
        if count:
            safe = f"{safe}_{count}"
        seen[safe] = count + 1
        sanitized_columns.append(safe)
    return df.toDF(*sanitized_columns)


# Function to detect BPS JSON metadata wrappers that should not be loaded into Bronze
def is_bps_metadata_json(file_path):
    if not file_path.lower().endswith('.json'):
        return False
    try:
        import json
        with open(file_path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
    except Exception:
        return False

    if isinstance(payload, dict):
        data = payload.get('data')
        if isinstance(data, dict) and 'excel' in data and 'table' in data and 'status' in payload:
            return True
    return False


# Function to process raw data to bronze layer
def process_to_bronze(file_name, file_format="csv"):
    raw_file_path = os.path.join(raw_data_path, file_name)
    
    if not os.path.exists(raw_file_path):
        print(f"File not found: {raw_file_path}")
        return

    # Read raw data
    print(f"Reading raw data from {raw_file_path}...")
    try:
        if file_format == "csv":
            df = spark.read.csv(raw_file_path, header=True, inferSchema=True)
        elif file_format == "json":
            df = spark.read.json(raw_file_path)
        elif file_format == "parquet":
            df = spark.read.parquet(raw_file_path)
        else:
            print(f"Unsupported format: {file_format}")
            return
    except Exception as e:
        print(f"Error reading file {file_name}: {e}")
        return

    # Extract table name from file name
    table_name = os.path.splitext(file_name)[0]
    output_path = os.path.join(bronze_path, table_name)

    # Save to Bronze Layer as Delta format (No data modification)
    print(f"Saving to Bronze Layer: {output_path}...")
    df = sanitize_dataframe_columns(df)
    df.write.format("delta").mode("overwrite").save(output_path)
    print(f"Successfully saved {table_name} to Bronze layer.\n")

if __name__ == "__main__":
    print("Starting Bronze Layer Processing...\n")
    
    # List available files in raw_data and process them automatically
    if os.path.exists(raw_data_path):
        data_files = []
        for root, _, files in os.walk(raw_data_path):
            for f in files:
                if f.endswith(('.csv', '.json', '.parquet')):
                    rel_path = os.path.relpath(os.path.join(root, f), raw_data_path)
                    data_files.append(rel_path)

        if not data_files:
            print("No data files found in raw_data/. Please put raw files there.")
        else:
            print(f"Files found in raw_data: {data_files}")
            for rel in data_files:
                file_path = os.path.join(raw_data_path, rel)
                if rel.lower().endswith('.json') and is_bps_metadata_json(file_path):
                    print(f"Skipping BPS metadata JSON wrapper: {file_path}")
                    continue

                ext = rel.lower().split('.')[-1]
                if ext == 'csv':
                    process_to_bronze(rel, 'csv')
                elif ext == 'json':
                    process_to_bronze(rel, 'json')
                elif ext == 'parquet':
                    process_to_bronze(rel, 'parquet')
    else:
        print(f"Directory {raw_data_path} does not exist.")

    print("Bronze Layer Processing completed.")
