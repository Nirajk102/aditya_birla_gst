#api_operations.py
from infrastructure.database_operations import database_operations
import json
import subprocess
import time
import pandas as pd
import requests
import os
import sys
import httpx
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

    # def get_file_ingestion_status(activity_id, template_type):
    #     try:
    #         # import pdb
    #         # pdb.set_trace()
    #         tenant = 'GSTSALES' if template_type == 'sales' else 'MAXITC'
    #         request_url = f'{BASE_URL}/integration/v1/ingest/file/{template_type}/status/{activity_id}?tenant={tenant}'
    #         request_headers = {'x-cleartax-auth-token': X_CLEARTAX_AUTH_TOKEN}
            
    #         response = requests.get(request_url, headers=request_headers, stream=True)
    #         # response = httpx.get(request_url, headers=request_headers)
    #         # import time
    #         # time.sleep(10)
    #         # Force load response
    #         # import pdb
    #         # pdb.set_trace()
    #         # raw_content = response.content  # blocks until full content received
    #         # print("Raw content length:", len(raw_content))
    #         print(response.headers)
    #         response.raise_for_status()
    #         response_data = response.json()
    #         print(response_data)
    #         if response.status_code == 200:
    #             print("File has been ingested successfully:", response_data)
    #             return response_data.get('status')
    #         else:
    #             print("Failure:", response_data.get('status'))
    #     except requests.exceptions.RequestException as e:
    #         print("An error occurred while getting file ingestion status:", e)

    # # def chack_response_api_opration():
    # #     print("response_api_oprations")


    import subprocess
    import json

    # def get_file_ingestion_status(activity_id, template_type):
    #     try:
    #         tenant = 'GSTSALES' if template_type == 'sales' else 'MAXITC'
    #         request_url = f'{BASE_URL}/integration/v1/ingest/file/{template_type}/status/{activity_id}?tenant={tenant}'
            
    #         curl_command = [
    #             'curl',
    #             '-X', 'GET',
    #             request_url,
    #             '-H', f'x-cleartax-auth-token: {X_CLEARTAX_AUTH_TOKEN}'
    #         ]
    #         print(f"curl_command , {curl_command}")
    #         # pdb.set_trace()
    #         import time
    #         time.sleep(10)

    #         result = subprocess.run(curl_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    #         if result.returncode != 0:
    #             print("Curl command failed:", result.stderr)
    #             return None

    #         print("Raw response:", result.stdout)

    #         response_data = json.loads(result.stdout)
    #         print(response_data)

    #         if 'status' in response_data:
    #             print("File has been ingested successfully:", response_data['status'])
    #             import pandas as pd
    #             file_error = response_data.get('fileError', {})


    #             # Optionally download the error CSV
    #             if 'errorFileUrl' in file_error:
    #                 error_file_url = file_error['errorFileUrl']
    #                 print(f"\nDownloading error file from:\n{error_file_url}\n")

    #                 response = requests.get(error_file_url)
    #                 if response.status_code == 200:
    #                     file_name = file_error['inputFileName'].replace('error01.xlsx', 'new_errors.csv')
    #                     with open(file_name, 'wb') as f:
    #                         f.write(response.content)
    #                     print(f"Error file downloaded successfully as '{file_name}'")

    #                     # Read the CSV into a DataFrame
    #                     df_errors = pd.read_csv(file_name)
    #                     print("\nFirst 5 rows of the error DataFrame:\n", df_errors.head())
    #                 else:
    #                     print("Failed to download error file.")
    #             return response_data['status']
    #         else:
    #             print("Failure:", response_data)

    #     except Exception as e:
    #         print("An error occurred while getting file ingestion status:", e)


    import subprocess
    import json
    import requests
    import pandas as pd
    import time


    def get_file_ingestion_status(activity_id, template_type):
        try:
            tenant = 'GSTSALES' if template_type == 'sales' else 'MAXITC'
            request_url = f'{BASE_URL}/integration/v1/ingest/file/{template_type}/status/{activity_id}?tenant={tenant}'

            curl_command = [
                'curl',
                '-X', 'GET',
                request_url,
                '-H', f'x-cleartax-auth-token: {X_CLEARTAX_AUTH_TOKEN}'
            ]
            print(f"\n🚀 Running curl command:\n{' '.join(curl_command)}\n")

            time.sleep(10)

            result = subprocess.run(curl_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            if result.returncode != 0:
                print("❌ Curl command failed:", result.stderr)
                return None

            response_data = json.loads(result.stdout)
            print("📦 Parsed response:\n", response_data)

            # Check for status
            status = response_data.get('status', 'UNKNOWN')
            print(f"📌 File ingestion status: {status}")
            # 🔍 Check invalidRows from processingStats
            stats = response_data.get('processingStats', [{}])[0]
            invalid_rows = stats.get('invalidRows', -1)

            conn = database_operations.connect_to_database()
            if not conn:
                print("❌ Failed to connect to database.")
                return status

            if invalid_rows == 0:
                if tenant == 'GSTSALES':
                    print("✅ No invalid rows. Updating all document numbers to '1' for the gstr1.")
                    database_operations.GSTR_1_update_status_for_document_numbers(conn, [])
                   
                else:
                    database_operations.GSTR_2_update_status_for_document_numbers(conn, [])
                    print("✅ All document numbers set to '1' for the gstr2.")
                conn.close()
                return status

            # Process error file if available
            file_error = response_data.get('fileError', {})
            error_file_url = file_error.get('errorFileUrl')
            input_file_name = file_error.get('inputFileName', 'error_file.xlsx')

            if error_file_url:
                print(f"\n⬇️ Downloading error file from:\n{error_file_url}\n")

                error_response = requests.get(error_file_url)
                if error_response.status_code == 200:
                    file_name = input_file_name.replace('.xlsx', '_errors.csv')
                    with open(file_name, 'wb') as f:
                        f.write(error_response.content)
                    print(f"✅ Error file downloaded successfully as '{file_name}'")

                    # Read the error CSV
                    try:
                        df_errors = pd.read_csv(file_name)
                        # Read only the headers
                        headers = pd.read_csv(file_name, nrows=0).columns.tolist()
                        document_numbers = df_errors['DOCUMENTNUMBER'].tolist()
                        print("📄 DOCUMENTNUMBER list:\n", document_numbers)
                        
                        # Establish database connection before updating document numbers
                        conn = database_operations.connect_to_database()
                        if conn:
                            if tenant == 'GSTSALES':
                                database_operations.GSTR_1_update_status_for_document_numbers(conn, document_numbers)
                                print("✅ Document numbers updated for gstr1.")
                            else:
                                database_operations.GSTR_2_update_status_for_document_numbers(conn, document_numbers)
                                print("✅ Document numbers updated for gstr2.")

                            conn.close()
                        else:
                            print("❌ Failed to connect to database for updating document numbers.")

                        print("📋 CSV Headers:\n", headers)
                        print("\n📊 First 5 rows of the error DataFrame:\n", df_errors.head())
                    except Exception as read_err:
                        print("⚠️ Failed to read the downloaded CSV:", read_err)
                else:
                    print("❌ Failed to download error file. Status code:", error_response.status_code)

            return status

        except Exception as e:
            print("🔥 An error occurred while getting file ingestion status:", e)
            return None
