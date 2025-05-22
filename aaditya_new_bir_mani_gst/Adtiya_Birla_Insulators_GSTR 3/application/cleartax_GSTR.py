# application/main.py

from infrastructure.api_operations import api_operations
# -------------------------------------------------------------------------------------------------
from infrastructure.database_operations import database_operations as db_ops
import datetime
import tempfile
import os
# Get the current date
current_datetime = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


class cleartax_GSTR:

    
    def fileGSTR1():
    
        start_time = start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        try:
            with tempfile.TemporaryDirectory() as temp_dir:
            # Define the temporary file path
                temp_file_path = os.path.join(temp_dir, f"GSTR{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")

            # # Connect to the database
            conn = db_ops.connect_to_database()
            if conn is None:
                raise ConnectionError("Failed to connect to the database")

            # Fetch data from the database
            df = db_ops.fetch_data_from_database_GSTR1(conn)
            if df is None:
                raise Exception("No data fetched from database")

            
            # Export DataFrame to Excel file
            export_successful = db_ops.export_to_excel(df,temp_file_path)
            if not export_successful:
                raise Exception("Failed to create Excel file")

            # Close Oracle connection
            # conn.close()
            end_time = end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


            # Fetch pre-signed URL
            pre_signed_url = api_operations.get_pre_signed_url(os.path.basename(temp_file_path), "XLSX", "sales")
            if not pre_signed_url:
                db_ops.insert_audit_log(
                    "GSTR1", os.path.basename(temp_file_path), "FAILED", "N/A", "N/A",
                    "N/A", "N/A", start_time, end_time, "Failed to get pre-signed URL", "FAILED",conn
                )
                return

            # Upload file to storage
            upload_successful = api_operations.upload_file_to_storage(temp_file_path, pre_signed_url, "XLSX")
            if not upload_successful:
                db_ops.insert_audit_log(
                    "GSTR1", os.path.basename(temp_file_path), "SUCCESS", "FAILED", "N/A",
                    "N/A", "N/A", start_time, end_time, "Failed to upload file to storage", "FAILED",conn
                )
                return

            # Trigger file ingestion
            activity_id = api_operations.trigger_file_ingestion(pre_signed_url, os.path.basename(temp_file_path), "sales")
            if not activity_id:
                db_ops.insert_audit_log(
                    "GSTR1", os.path.basename(temp_file_path), "SUCCESS", "SUCCESS", "N/A",
                    "FAILED", "N/A", start_time, end_time, "Failed to trigger file ingestion", "FAILED",conn
                )
                return

            # Check file ingestion status
            ingestion_status = api_operations.get_file_ingestion_status(activity_id, "sales")
            if not ingestion_status:
                db_ops.insert_audit_log(
                    "GSTR1", os.path.basename(temp_file_path), "SUCCESS", "SUCCESS", activity_id,
                    "SUCCESS", "FAILED", start_time, end_time, "Failed to get file ingestion status", "FAILED",conn
                )
                return

            db_ops.insert_audit_log(
                "GSTR1", os.path.basename(temp_file_path), "SUCCESS", "SUCCESS", activity_id,
                "SUCCESS", "SUCCESS", start_time, end_time, "GSTR1 process completed successfully", "SUCCESS",conn
            )
            print("GSTR1 successful")

        except Exception as e:
            end_time = end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            db_ops.insert_audit_log(
                "GSTR1", os.path.basename(temp_file_path), "N/A", "N/A", "N/A",
                "N/A", "N/A", start_time, end_time, str(e), "FAILED",conn
            )
            print("An error occurred:", e)
# ----------------------------------------------------------------------------------------------------------------

    def fileGSTR2():
    
        start_time = start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


        try:
            with tempfile.TemporaryDirectory() as temp_dir:
            # Define the temporary file path
                temp_file_path = os.path.join(temp_dir, f"GSTR{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")

            # # Connect to the database
            conn = db_ops.connect_to_database()
            if conn is None:
                raise ConnectionError("Failed to connect to the database")

            # Fetch data from the database
            df = db_ops.fetch_data_from_database_GSTR2(conn)
            if df is None:
                raise Exception("No data fetched from database")

            
            # Export DataFrame to Excel file
            export_successful = db_ops.export_to_excel(df,temp_file_path)
            if not export_successful:
                raise Exception("Failed to create Excel file")

            # Close Oracle connection
            # conn.close()
            end_time = end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


            # Fetch pre-signed URL
            pre_signed_url = api_operations.get_pre_signed_url(os.path.basename(temp_file_path), "XLSX", "purchase")
            if not pre_signed_url:
                db_ops.insert_audit_log(
                    "GSTR2", os.path.basename(temp_file_path), "FAILED", "N/A", "N/A",
                    "N/A", "N/A", start_time, end_time, "Failed to get pre-signed URL", "FAILED",conn
                )
                return

            # Upload file to storage
            upload_successful = api_operations.upload_file_to_storage(temp_file_path, pre_signed_url, "XLSX")
            if not upload_successful:
                db_ops.insert_audit_log(
                    "GSTR2", os.path.basename(temp_file_path), "SUCCESS", "FAILED", "N/A",
                    "N/A", "N/A", start_time, end_time, "Failed to upload file to storage", "FAILED",conn
                )
                return

            # Trigger file ingestion
            activity_id = api_operations.trigger_file_ingestion(pre_signed_url, os.path.basename(temp_file_path), "purchase")
            if not activity_id:
                db_ops.insert_audit_log(
                    "GSTR2", os.path.basename(temp_file_path), "SUCCESS", "SUCCESS", "N/A",
                    "FAILED", "N/A", start_time, end_time, "Failed to trigger file ingestion", "FAILED",conn
                )
                return

            # Check file ingestion status
            ingestion_status = api_operations.get_file_ingestion_status(activity_id, "purchase")
            # import pdb
            # pdb.set_trace()
            if not ingestion_status:
                db_ops.insert_audit_log(
                    "GSTR2", os.path.basename(temp_file_path), "SUCCESS", "SUCCESS", activity_id,
                    "SUCCESS", "FAILED", start_time, end_time, "Failed to get file ingestion status", "FAILED",conn
                )
                return

            db_ops.insert_audit_log(
                "GSTR2", os.path.basename(temp_file_path), "SUCCESS", "SUCCESS", activity_id,
                "SUCCESS", "SUCCESS", start_time, end_time, "GSTR2 process completed successfully", "SUCCESS",conn
            )
            print("GSTR2 successful")

        except Exception as e:
            end_time = end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            db_ops.insert_audit_log(
                "GSTR2", os.path.basename(temp_file_path), "N/A", "N/A", "N/A",
                "N/A", "N/A", start_time, end_time, str(e), "FAILED",conn
            )
            print("An error occurred:", e)    





























# try:
#     # Connect to the database
#     conn = db_conn.connect_to_database()
#     if conn is None:
#         exit(1)

#     # Fetch data from the database
#     df = db_op.fetch_data_from_database(conn)
#     if df is None:
#         exit(1)

#     # Print the DataFrame to the console
#     print("Data from the database:")
#     print(df)  # This line prints both field names and entire data

#     # Export DataFrame to Excel file
#     excel_filename = 'D:/CLEARTAX/Adtiy Birla/Integrationcode2/infrastructure/xyzsale.xlsx'
#     db_op.export_to_excel(df, excel_filename)

# except Exception as e:
#     print("An error occurred:", e)



























# from infrastructure.api_oprations import api_oprations
# from infrastructure.database_oprations import database_oprations
# from infrastructure.file_oprations import file_operations

# class cleartaxGSTR:   
#     @staticmethod
#     def cleartax_GSTR():
#         return api_oprations.chack_response_api_opration()

#     @staticmethod
#     def cleartax_GSTR1():
#         return database_oprations.chack_response_database_opraions()

#     @staticmethod
#     def cleartax_GSTR2():
#         return file_operations.chack_response_file_oprations()

# if __name__ == "__main__":
#     cleartaxGSTR.cleartax_GSTR()
#     cleartaxGSTR.cleartax_GSTR1()
#     cleartaxGSTR.cleartax_GSTR2()
