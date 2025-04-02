import logging
import azure.functions as func
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import os
import tempfile

# Connect to Data Lake (using connection string or credential)
def read_parquet_from_datalake():
    # Recommended: Use environment variables for these
    storage_account_name = "icezydatalake01"
    file_system_name = "raw-data"
    file_path = "analytics_output/session_activity/part-00000-*.parquet"
    credential = os.environ["AZURE_STORAGE_KEY"]  # or use DefaultAzureCredential for managed identity

    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=credential
    )

    file_system = service_client.get_file_system_client(file_system_name)
    paths = file_system.get_paths(path="analytics_output/session_activity")
    
    # Read the first parquet file found
    for path in paths:
        if path.name.endswith(".parquet"):
            file_client = file_system.get_file_client(path.name)
            download = file_client.download_file()
            downloaded_bytes = download.readall()

            # Save to temp file and read as DataFrame
            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                tmp.write(downloaded_bytes)
                tmp_path = tmp.name

            df = pd.read_parquet(tmp_path, engine="pyarrow")
            return df.to_dict(orient="records")

    return []

# Azure HTTP function
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="getSessionActivity")
def get_session_activity(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = read_parquet_from_datalake()
        return func.HttpResponse(body=str(data), status_code=200)
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse("Error reading data.", status_code=500)