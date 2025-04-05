import os
import json
from flask import Flask, jsonify, request
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Load credentials
account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_STORAGE_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")
print("account_name:", account_name)
print("container_name:", container_name)

# Connect to Data Lake
service_client = DataLakeServiceClient(
    account_url=f"https://{account_name}.dfs.core.windows.net",
    credential=account_key
)

def read_parquet_folder(folder_name):
    file_system_client = service_client.get_file_system_client(file_system=container_name)
    paths = file_system_client.get_paths(path=folder_name)
    
    dataframes = []
    for path in paths:
        if path.name.endswith(".parquet"):
            file_client = file_system_client.get_file_client(path.name)
            download = file_client.download_file()
            stream = BytesIO()
            download.readinto(stream)
            stream.seek(0)
            table = pq.read_table(stream)
            df = table.to_pandas()
            dataframes.append(df)
    
    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    else:
        return pd.DataFrame()

@app.route("/")
def home():
    return jsonify({
        "message": "Welcome to the Azure Data Lake API",
        "endpoints": ["/brand_sales", "/event_spent", "/session_activity", "/viewed_product"]
    })

@app.route("/brand_sales")
def brand_sales():
    df = read_parquet_folder("brand_sales")
    result = df.groupby("brand")["total_cart_value"].sum().reset_index()
    limit = request.args.get("limit", default=None, type=int)
    if limit:
        result = result.nlargest(limit, "total_cart_value")
    return app.response_class(
        response=json.dumps(result.to_dict(orient="records"), indent=4),
        mimetype='application/json'
    )

@app.route("/event_spent")
def event_spent():
    df = read_parquet_folder("event_spent")
    result = df.groupby("event_type")["total_cart_value"].sum().reset_index()
    limit = request.args.get("limit", default=None, type=int)
    if limit:
        result = result.nlargest(limit, "total_cart_value")
    return app.response_class(
        response=json.dumps(result.to_dict(orient="records"), indent=4),
        mimetype='application/json'
    )

@app.route("/session_activity")
def session_activity():
    df = read_parquet_folder("session_activity")
    result = df["event_type"].value_counts().reset_index()
    result.columns = ["event_type", "count"]
    limit = request.args.get("limit", default=None, type=int)
    if limit:
        result = result.head(limit)
    return app.response_class(
        response=json.dumps(result.to_dict(orient="records"), indent=4),
        mimetype='application/json'
    )

@app.route("/viewed_product")
def viewed_product():
    df = read_parquet_folder("viewed_product")
    result = df["product_id"].value_counts().reset_index()
    result.columns = ["product_id", "views"]
    limit = request.args.get("limit", default=None, type=int)
    if limit:
        result = result.head(limit)
    return app.response_class(
        response=json.dumps(result.to_dict(orient="records"), indent=4),
        mimetype='application/json'
    )

if __name__ == "__main__":
    app.run(debug=False)