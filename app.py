import os
import pandas as pd
from flask import Flask, jsonify
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Flask app
app = Flask(__name__)

# Environment variables
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")

# Debug print
print("account_name:", account_name)
print("container_name:", container_name)

# Create DataLake client
service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=account_key
)

# Function to read partitioned Parquet folder
def read_parquet_folder(folder_path):
    file_system_client = service_client.get_file_system_client(file_system=container_name)
    paths = file_system_client.get_paths(path=folder_path)
    df_list = []

    for path in paths:
        if path.name.endswith(".parquet"):
            file_client = file_system_client.get_file_client(path.name)
            download = file_client.download_file()
            bytes_data = download.readall()
            table = pq.read_table(BytesIO(bytes_data))
            df = table.to_pandas()
            df_list.append(df)

    return pd.concat(df_list, ignore_index=True)

# Endpoint: /brand_sales
@app.route("/brand_sales")
def brand_sales():
    df = read_parquet_folder("analytics_output/brand_sales")
    return jsonify(df.head(20).to_dict(orient="records"))

# Endpoint: /event_spent
@app.route("/event_spent")
def event_spent():
    df = read_parquet_folder("analytics_output/event_spent")
    return jsonify(df.head(20).to_dict(orient="records"))

# Endpoint: /session_activity
@app.route("/session_activity")
def session_activity():
    df = read_parquet_folder("analytics_output/session_activity")
    return jsonify(df.head(20).to_dict(orient="records"))

# Endpoint: /viewed_product
@app.route("/viewed_product")
def viewed_product():
    df = read_parquet_folder("analytics_output/viewed_product")
    return jsonify(df.head(20).to_dict(orient="records"))

# Run Flask
if __name__ == "__main__":
    app.run(debug=False)