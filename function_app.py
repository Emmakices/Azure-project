import azure.functions as func
import logging
import json
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

account_name = "icezydatalake01"
file_system = "raw-data"
file_path = "analytics_output/session_activity/part-00000.parquet"  # Adjust as needed

def get_file_data():
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=credential)
    file_client = service_client.get_file_system_client(file_system).get_file_client(file_path)
    
    download = file_client.download_file()
    data = download.readall().decode("utf-8")  # assumes CSV/JSON; for Parquet, you'll need to parse binary differently
    return data

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="getSessionActivity")
def getSessionActivity(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Received HTTP request for session activity')

    try:
        result = get_file_data()
        return func.HttpResponse(result, mimetype="application/json")
    except Exception as e:
        logging.error(f"Failed: {e}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)