#api_operations.py

import requests
import os
import sys

# Add the main folder to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import load_config

# Load the configuration based on environment
# This will be loaded when the module is imported
env = os.environ.get('ENVIRONMENT', '').lower()
config = load_config(env)

# Access API details
X_CLEARTAX_AUTH_TOKEN = config['API_DETAILS']['CLEARTAX_AUTH_TOKEN']
BASE_URL = config['API_DETAILS']['BASE_URL']

# def fetch_data():
#     headers = {
#         'Authorization': X_CLEARTAX_AUTH_TOKEN
#     }
#     response = requests.get(f"{BASE_URL}/your-endpoint", headers=headers)
    
#     if response.status_code == 200:
#         print("Data fetched successfully:", response.json())
#     else:
#         print("Failed to fetch data:", response.status_code, response.text)

# # Call the function
# if __name__ == "__main__":
#     fetch_data()

# ---------------------------------------------------------------
class api_operations:

    def get_pre_signed_url(filename, file_content_type, template_type):
        try:
            cleartax_headers = {
                'X-cleartax-auth-token': X_CLEARTAX_AUTH_TOKEN,
                'fileContentType': file_content_type
            }
            request_url = f'{BASE_URL}/integration/v1/generatePreSign/{template_type}?fileName={filename}'
            response = requests.get(request_url, headers=cleartax_headers)
            response.raise_for_status()
            response_data = response.json()
            status = response_data.get('status')
            pre_signed_s3_url = response_data.get('preSignedS3Url')
            if status == 'CREATED':
                return pre_signed_s3_url
        except requests.exceptions.RequestException as e:
            print("An error occurred while getting the pre-signed URL:", e)
        return None

    def upload_file_to_storage(file_path, pre_signed_url, file_content_type):
        try:
            content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' if file_content_type == 'XLSX' else 'application/vnd.ms-excel'
            cleartax_headers = {'Content-Type': content_type}
            
            with open(file_path, 'rb') as file:
                response = requests.put(pre_signed_url, data=file, headers=cleartax_headers)
            
            if response.status_code == 200:
                print("File uploaded successfully.")
                return True
            else:
                print("Error uploading file. Status code:", response.status_code)
        except requests.exceptions.RequestException as e:
            print("An error occurred while uploading file to storage:", e)
        return False

    def trigger_file_ingestion(pre_signed_url, filename_extension, template_type):
        try:
            request_url = f'{BASE_URL}/integration/v1/ingest/file/{template_type}'
            request_headers = {'x-cleartax-auth-token': X_CLEARTAX_AUTH_TOKEN}
            template_id = '618a5623836651c01c1498ad' if template_type == 'sales' else '60e5613ff71f4a7aeca4336b'

            request_body = {
                "userInputArgs": {
                    "gstins": [],
                    "templateId": template_id
                },
                "fileInfo": {
                    "s3FileUrl": pre_signed_url,
                    "userFileName": filename_extension
                }
            }

            response = requests.post(request_url, headers=request_headers, json=request_body)
            response.raise_for_status()
            response_data = response.json()
            activity_id = response_data.get('activityId')
            if response.status_code == 201:
                print("File ingestion triggered successfully.")
                return activity_id
        except requests.exceptions.RequestException as e:
            print("An error occurred while triggering file ingestion:", e)
        return None

    def get_file_ingestion_status(activity_id, template_type):
        try:
            tenant = 'GSTSALES' if template_type == 'sales' else 'MAXITC'
            request_url = f'{BASE_URL}/integration/v1/ingest/file/{template_type}/status/{activity_id}?tenant={tenant}'
            request_headers = {'x-cleartax-auth-token': X_CLEARTAX_AUTH_TOKEN}
            
            response = requests.get(request_url, headers=request_headers)
            response.raise_for_status()
            response_data = response.json()
            
            if response.status_code == 200:
                print("File has been ingested successfully:", response_data)
                return response_data.get('status')
            else:
                print("Failure:", response_data.get('status'))
        except requests.exceptions.RequestException as e:
            print("An error occurred while getting file ingestion status:", e)

    # def chack_response_api_opration():
    #     print("response_api_oprations")