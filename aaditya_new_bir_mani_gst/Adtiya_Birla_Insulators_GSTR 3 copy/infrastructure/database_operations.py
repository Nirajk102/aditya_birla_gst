import logging
# import cx_Oracle  # Commented out Oracle
import mysql.connector  # Added MySQL connector
import pandas as pd
import uuid
import os
import sys
import datetime

# Add the main folder to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import load_config

# Load the configuration based on environment
# This will be loaded when the module is imported
env = os.environ.get('ENVIRONMENT', '').lower()
config = load_config(env)

# Access database connection details
DB_user = config['DB_CONNECTION']['db_user']
db_password = config['DB_CONNECTION']['db_password']
db_host = config['DB_CONNECTION']['db_host']
db_port = config['DB_CONNECTION']['db_port']
db_database = config['DB_CONNECTION']['db_sid']  # Using db_sid as database name

# def connect_to_database():
#     print(f"Connecting to database at {db_host}:{db_port} as user {DB_user}...")
#     # Add your database connection logic here

# Call the function
# if __name__ == "__main__":
#     connect_to_database()

# -----------------------------------------------------------------

# from dotenv import load_dotenv
# # Load variables from .env file
# load_dotenv()

# DB_user = os.getenv("db_user")
# db_password = os.getenv("db_password")
# db_host = os.getenv("db_host")
# db_port = os.getenv("db_port")
# db_sid = os.getenv("db_sid")

class database_operations:


    # def connect_to_database():
    #     try:
    #         # Oracle database connection details
    #         dsn_tns = cx_Oracle.makedsn('localhost', '1521', sid='orcl')
    #         conn = cx_Oracle.connect(user='C##neelmani', password='Engineo$%9450', dsn=dsn_tns)
    #         return conn
    #     except cx_Oracle.DatabaseError as e:
    #         print("An error occurred while connecting to the database:", e)
    #         return None
    @staticmethod
    def connect_to_database():
        try:
            # Connect to MySQL database
            print(f"Connecting to MySQL database at {db_host}:{db_port} as user {DB_user}...")
            conn = mysql.connector.connect(
                host=db_host,
                port=int(db_port),
                user=DB_user,
                password=db_password,
                database=db_database
            )
            print("Connected to the MySQL database!")
            return conn
        except mysql.connector.Error as e:
            print("An error occurred while connecting to the MySQL database:", e)
            return None
            
    # Original Oracle connection method (commented out)
    # @staticmethod
    # def connect_to_database_oracle():
    #     try:
    #         # Create the DSN string
    #         dsn_tns = cx_Oracle.makedsn(db_host, db_port, sid=db_sid)
    #         print(f"Connecting to database at {db_host}:{db_port} as user {DB_user}...")
    #         conn = cx_Oracle.connect(user=DB_user, password=db_password, dsn=dsn_tns)
    #         print("Connected to the database!")
    #         return conn
    #     except cx_Oracle.DatabaseError as e:
    #         print("An error occurred while connecting to the database:", e)
    #         return None

    

    def fetch_data_from_database_GSTR1(conn):
        try:
            # SQL query to fetch data - using column names from gstr_data_1 table
            query = """
                    SELECT
                        DOCUMENTTYPE as DocumentType,
                        DOCUMENTNUMBER as DocumentNumber,
                        DOCUMENTDATE as DocumentDate,
                        RETURNFILINGMONTH as ReturnFilingMonth,
                        PLACEOFSUPPLY as PlaceofSupply,
                        APPLICABLETAXRATE as ApplicableTaxRate,
                        ISBILLOFSUPPLY as IsBillofSupply,
                        ISREVERSECHARGE as IsReverseCharge,
                        ISGSTTDSDEDUCTED as IsGSTTDSDeducted,
                        LINKEDADVANCEDOCUMENTNUMBER as LinkedAdvanceDocumentNumber,
                        LINKEDADVANCEDOCUMENTDATE as LinkedAdvanceDocumentDate,
                        LINKEDADVANCEADJUSTMENTAMOUNT as LinkedAdvanceAdjustmentAmount,
                        ORIGINALDOCUMENTNUMBER as OriginalDocumentNumber,
                        ORIGINALDOCUMENTDATE as OriginalDocumentDate,
                        ORIGINALDOCUMENTCUSTOMERGSTIN as OriginalDocumentCustomerGSTIN,
                        LINKEDINVOICENUMBER as LinkedInvoiceNumber,
                        LINKEDINVOICEDATE as LinkedInvoiceDate,
                        LINKEDINVOICECUSTOMERGSTIN as LinkedInvoiceCustomerGSTIN,
                        ISLINKEDINVOICEPREGST as IsLinkedInvoicePreGST,
                        REASONFORISSUINGCDN as ReasonforIssuingCDN,
                        ECOMMERCEGSTIN as EcommerceGSTIN,
                        SUPPLIERGSTIN as SupplierGSTIN,
                        SUPPLIERNAME as SupplierName,
                        CUSTOMERGSTIN as CustomerGSTIN,
                        CUSTOMERNAME as CustomerName,
                        CUSTOMERADDRESS as CustomerAddress,
                        CUSTOMERCITY as CustomerCity,
                        CUSTOMERSTATE as CustomerState,
                        CUSTOMERTAXPAYERTYPE as CustomerTaxpayerType,
                        ITEMDESCRIPTION as ItemDescription,
                        ITEMCATEGORY as ItemCategory,
                        HSNORSACCODE as HSNorSACcode,
                        ITEMQUANTITY as ItemQuantity,
                        ITEMUNITCODE as ItemUnitCode,
                        ITEMUNITPRICE as ItemUnitPrice,
                        ITEMDISCOUNTAMOUNT as ItemDiscountAmount,
                        ITEMTAXABLEAMOUNT as ItemTaxableAmount,
                        ZEROTAXCATEGORY as ZeroTaxCategory,
                        GSTRATE as GSTRate,
                        CGSTRATE as CGSTRate,
                        CGSTAMOUNT as CGSTAmount,
                        SGSTRATE as SGSTRate,
                        SGSTAMOUNT as SGSTAmount,
                        IGSTRATE as IGSTRate,
                        IGSTAMOUNT as IGSTAmount,
                        CESSRATE as CESSRate,
                        CESSAMOUNT as CESSAmount,
                        DOCUMENTCGSTAMOUNT as DocumentCGSTAmount,
                        DOCUMENTSGSTAMOUNT as DocumentSGSTAmount,
                        DOCUMENTIGSTAMOUNT as DocumentIGSTAmount,
                        DOCUMENTCESSAMOUNT as DocumentCessAmount,
                        DOCUMENTTOTALAMOUNT as DocumentTotalAmount,
                        EXPORTTYPE as ExportType,
                        EXPORTBILLNUMBER as ExportBillNumber,
                        EXPORTBILLDATE as ExportBillDate,
                        EXPORTPORTCODE as ExportPortCode,
                        ERPSOURCE as ERPSource,
                        COMPANYCODE as CompanyCode,
                        VOUCHERTYPE as VoucherType,
                        VOUCHERNUMBER as VoucherNumber,
                        VOUCHERDATE as VoucherDate,
                        ISDOCUMENTCANCELLED as IsDocumentCancelled,
                        ISDOCUMENTDELETED as IsDocumentDeleted,
                        EXTERNALID as ExternalID,
                        EXTERNALLINEITEMID as ExternalLineitemID
                    FROM
                        gstr_data_1
                    WHERE
                        STATUS IS NULL OR status = '' OR status = 'FAILED'
                    """
             # for now i have changed the table name to clear tax gstr1
            # Create a cursor and execute the query
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query)
            
            # Fetch all rows and convert to DataFrame
            rows = cursor.fetchall()
            cursor.close()
            
            # Convert to DataFrame
            df = pd.DataFrame(rows) if rows else pd.DataFrame()
            return df
        except Exception as e:
            print("An error occurred while fetching data from the database:", e)
            return pd.DataFrame()  # Return empty DataFrame on error

    

    def fetch_data_from_database_GSTR2(conn):
        try:
            # SQL query to fetch data - using only columns that exist in gstr_data_1
            # query = """SELECT 
            #     DOCUMENTTYPE as DocumentType,
            #     DOCUMENTNUMBER as DocumentNumber,
            #     DOCUMENTDATE as DocumentDate,
            #     RETURNFILINGMONTH as ReturnFilingMonth,
            #     PLACEOFSUPPLY as PlaceofSupply,
            #     ISBILLOFSUPPLY as IsBillofSupply,
            #     ISREVERSECHARGE as IsReverseCharge,
            #     LINKEDADVANCEDOCUMENTNUMBER as LinkedAdvanceDocumentNumber,
            #     LINKEDADVANCEDOCUMENTDATE as LinkedAdvanceDocumentDate,
            #     LINKEDADVANCEADJUSTMENTAMOUNT as LinkedAdvanceAdjustmentAmount,
            #     LINKEDINVOICENUMBER as LinkedInvoiceNumber,
            #     LINKEDINVOICEDATE as LinkedInvoiceDate,
            #     SUPPLIERGSTIN as SupplierGSTIN,
            #     SUPPLIERNAME as SupplierName,
            #     CUSTOMERGSTIN as CustomerGSTIN,
            #     ITEMDESCRIPTION as ItemDescription,
            #     HSNORSACCODE as HSNorSACcode,
            #     ITEMQUANTITY as ItemQuantity,
            #     ITEMUNITCODE as ItemUnitCode,
            #     ITEMUNITPRICE as ItemUnitPrice,
            #     ITEMDISCOUNTAMOUNT as ItemDiscountAmount,
            #     ITEMTAXABLEAMOUNT as ItemTaxableAmount,
            #     ZEROTAXCATEGORY as ZeroTaxCategory,
            #     GSTRATE as GSTRate,
            #     CGSTRATE as CGSTRate,
            #     CGSTAMOUNT as CGSTAmount,
            #     SGSTRATE as SGSTRate,
            #     SGSTAMOUNT as SGSTAmount,
            #     IGSTRATE as IGSTRate,
            #     IGSTAMOUNT as IGSTAmount,
            #     CESSRATE as CESSRate,
            #     CESSAMOUNT as CESSAmount,
            #     DOCUMENTCGSTAMOUNT as DocumentCGSTAmount,
            #     DOCUMENTSGSTAMOUNT as DocumentSGSTAmount,
            #     DOCUMENTIGSTAMOUNT as DocumentIGSTAmount,
            #     DOCUMENTCESSAMOUNT as DocumentCessAmount,
            #     DOCUMENTTOTALAMOUNT as DocumentTotalAmount,
            #     EXPORTTYPE as ExportType,
            #     EXPORTBILLNUMBER as ExportBillNumber,
            #     EXPORTBILLDATE as ExportBillDate,
            #     EXPORTPORTCODE as ExportPortCode,
            #     ERPSOURCE as ERPSource,
            #     COMPANYCODE as CompanyCode,
            #     VOUCHERTYPE as VoucherType,
            #     VOUCHERNUMBER as VoucherNumber,
            #     VOUCHERDATE as VoucherDate,
            #     ISDOCUMENTCANCELLED as IsDocumentCancelled,
            #     ISDOCUMENTDELETED as IsDocumentDeleted,
            #     EXTERNALID as ExternalID,
            #     EXTERNALLINEITEMID as ExternalLineItemID
            # FROM 
            #     gstr_data
            # WHERE 
            #     STATUS IS NULL OR status = '' OR status = 'FAILED'"""
            #----------------------------------------------------------------------------------------------------------------------

            query = ''' SELECT
                    DOCUMENTTYPE,
                    DOCUMENTNUMBER,
                    DOCUMENTDATE,
                    RETURNFILINGMONTH,
                    PLACEOFSUPPLY,
                    ISBILLOFSUPPLY,
                    ISREVERSECHARGE,
                    LINKEDADVANCEDOCUMENTNUMBER,
                    LINKEDADVANCEDOCUMENTDATE,
                    LINKEDADVANCEADJUSTMENTAMOUNT,
                    LINKEDINVOICENUMBER,
                    LINKEDINVOICEDATE,
                    SUPPLIERGSTIN,
                    SUPPLIERNAME,
                    SUPPLIERADDRESS,
                    SUPPLIERCITY,
                    SUPPLIERSTATE,
                    ISSUPPLIERCOMPOSITIONDEALER,
                    CUSTOMERGSTIN,
                    ITEMCATEGORY,
                    ITEMDESCRIPTION,
                    HSNORSACCODE,
                    ITEMQUANTITY,
                    ITEMUNITCODE,
                    ITEMUNITPRICE,
                    ITEMDISCOUNTAMOUNT,
                    ITEMTAXABLEAMOUNT,
                    ZEROTAXCATEGORY,
                    GSTRATE,
                    CGSTRATE,
                    CGSTAMOUNT,
                    SGSTRATE,
                    SGSTAMOUNT,
                    IGSTRATE,
                    IGSTAMOUNT,
                    CESSRATE,
                    CESSAMOUNT,
                    ITCCLAIMTYPE,
                    ITCCLAIMCGSTAMOUNT,
                    ITCCLAIMSGSTAMOUNT,
                    ITCCLAIMIGSTAMOUNT,
                    ITCCLAIMCESSAMOUNT,
                    DOCUMENTCGSTAMOUNT,
                    DOCUMENTSGSTAMOUNT,
                    DOCUMENTIGSTAMOUNT,
                    DOCUMENTCESSAMOUNT,
                    DOCUMENTTOTALAMOUNT,
                    IMPORTTYPE,
                    IMPORTBILLNUMBER,
                    IMPORTBILLDATE,
                    IMPORTPORTCODE,
                    GOODSRECEIPTNOTENUMBER,
                    GOODSRECEIPTNOTEDATE,
                    GOODSRECEIPTNOTEQUANTITY,
                    GOODSRECEIPTNOTEAMOUNT,
                    PAYMENTDUEDATE,
                    ERPSOURCE,
                    COMPANYCODE,
                    VENDORCODE,
                    VOUCHERTYPE,
                    VOUCHERNUMBER,
                    VOUCHERDATE,
                    ISDOCUMENTCANCELLED,
                    ISDOCUMENTDELETED,
                    EXTERNALID,
                    EXTERNALLINEITEMID,
                    STATUS
                FROM gstr4_data_1
                WHERE STATUS IS NULL OR STATUS = '' OR STATUS = 'FAILED';
                '''



            # Create a cursor and execute the query
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query)
            
            # Fetch all rows and convert to DataFrame
            rows = cursor.fetchall()
            cursor.close()
            
            # Convert to DataFrame
            df = pd.DataFrame(rows) if rows else pd.DataFrame()
            return df

        except Exception as e:
            print("An error occurred while fetching data from the database:", e)
            return pd.DataFrame()  # Return empty DataFrame on error

    def export_to_excel(df, filename):
        # try:
        #     # Save DataFrame to Excel file
        #     df.to_excel(filename, index=True)
        #     print("Excel file created successfully at:", filename)
        #     return True
        # except Exception as e:
        #     print("An error occurred while exporting to Excel:", e)
        #     return False
        try:
        # Ensure the directory exists
            directory = os.path.dirname(filename)
            if not os.path.exists(directory):
                os.makedirs(directory)
            
            # Save DataFrame to Excel file
            df.to_excel(filename, index=True)
            print("Excel file created successfully at:", filename)
            return True
        except Exception as e:
            print("An error occurred while exporting to Excel:", e)
        return False

      

    
    def insert_audit_log(
        program_type, file_name, get_presigned_url_status, upload_file_status,
        activity_id, trigger_file_ingestion_status, get_file_ingestion_status,
        start_time, end_time, error_message, program_status, conn
        ):
        log_id = str(uuid.uuid4())

        # First, try to write to a log file as a fallback
        log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        log_file = os.path.join(log_dir, f'audit_log_{datetime.datetime.now().strftime("%Y%m%d")}.log')
        
        log_entry = (
            f"UUID: {log_id}\n"
            f"Time: {datetime.datetime.now()}\n"
            f"Program Type: {program_type}\n"
            f"File Name: {file_name}\n"
            f"Presigned URL Status: {get_presigned_url_status}\n"
            f"Upload Status: {upload_file_status}\n"
            f"Activity ID: {activity_id}\n"
            f"Trigger Ingestion Status: {trigger_file_ingestion_status}\n"
            f"Ingestion Status: {get_file_ingestion_status}\n"
            f"Start Time: {start_time}\n"
            f"End Time: {end_time}\n"
            f"Error Message: {error_message}\n"
            f"Program Status: {program_status}\n"
            f"{'=' * 50}\n"
        )
        
        with open(log_file, 'a') as f:
            f.write(log_entry)
        
        # Now try to insert into the database if possible
        try:
            # MySQL uses %s placeholders instead of named parameters
            insert_query = """
            INSERT INTO AuditLogTable (
                UUID, ProgramType, FileName, GetPresignedURLAPIStatus, 
                UploadFileToStorageAPIStatus, Activity_id, TriggerFileIngestionAPIStatus, 
                GetFileIngestionStatus, StartTime, EndTime, ErrorMessage, ProgramStatus
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, 
                STR_TO_DATE(%s, '%%Y-%%m-%%d %%H:%%i:%%s'), 
                STR_TO_DATE(%s, '%%Y-%%m-%%d %%H:%%i:%%s'), 
                %s, %s
            )
            """

            # For MySQL, parameters are passed as a tuple in order
            params = (
                log_id,
                program_type,
                file_name,
                get_presigned_url_status,
                upload_file_status,
                activity_id,
                trigger_file_ingestion_status,
                get_file_ingestion_status,
                start_time,
                end_time,
                error_message,
                program_status
            )

            # Create cursor
            cursor = conn.cursor()
            
            # Execute insert query
            cursor.execute(insert_query, params)
            print(f"Audit log for file '{file_name}' inserted successfully.")

            # Try to update status in gstr_data_1 table if it exists
            try:
                update_query = """
                UPDATE gstr4_data_1
                SET Status = %s
                WHERE Status IN ('', 'FAILED', NULL)¯
                WHERE Status IS NULL OR Status = '' OR Status = '¯FAILED'
                """
                cursor.execute(update_query, (program_status,))
                print(f"Program status updated for '{file_name}' in table 'gstr4_data_1' successfully.")
            except Exception as e:
                logging.error(f"Error updating table 'gstr4_data_1': {e}")
            
            # Commit changes
            conn.commit()
            
        except Exception as e:
            logging.error(f"An error occurred while inserting audit log to database: {e}")
            print(f"Audit log saved to file: {log_file}")
            if conn:
                try:
                    conn.rollback()  # Rollback on error
                except:
                    pass

        finally:
            try:
                if 'cursor' in locals() and cursor:
                    cursor.close()  # Close cursor
            except:
                pass
            
    # Original Oracle version (commented out)
    # def insert_audit_log_oracle(
    #     program_type, file_name, get_presigned_url_status, upload_file_status,
    #     activity_id, trigger_file_ingestion_status, get_file_ingestion_status,
    #     start_time, end_time, error_message, program_status, conn
    #     ):
    #     log_id = str(uuid.uuid4())
    #
    #     insert_query = """
    #     INSERT INTO AuditLogTable (
    #         UUID, ProgramType, FileName, GetPresignedURLAPIStatus, 
    #         UploadFileToStorageAPIStatus, Activity_id, TriggerFileIngestionAPIStatus, 
    #         GetFileIngestionStatus, StartTime, EndTime, ErrorMessage, ProgramStatus
    #     ) VALUES (
    #         :log_id, :program_type, :file_name, :get_presigned_url_status, 
    #         :upload_file_status, :activity_id, :trigger_file_ingestion_status, 
    #         :get_file_ingestion_status, TO_DATE(:start_time, 'YYYY-MM-DD HH24:MI:SS'), 
    #         TO_DATE(:end_time, 'YYYY-MM-DD HH24:MI:SS'), :error_message, :program_status
    #     )
    #     """
    #
    #     params = {
    #         'log_id': log_id,
    #         'program_type': program_type,
    #         'file_name': file_name,
    #         'get_presigned_url_status': get_presigned_url_status,
    #         'upload_file_status': upload_file_status,
    #         'activity_id': activity_id,
    #         'trigger_file_ingestion_status': trigger_file_ingestion_status,
    #         'get_file_ingestion_status': get_file_ingestion_status,
    #         'start_time': start_time,
    #         'end_time': end_time,
    #         'error_message': error_message,
    #         'program_status': program_status
    #     }
    #
    #     try:
    #         # Using the connection context
    #         with conn:
    #             with conn.cursor() as cursor:
    #                 cursor.execute(insert_query, params)
    #                 print(f"Audit log for file '{file_name}' inserted successfully.")
    #
    #                 # Update statuses in ClearTax_GSTR1 and ClearTax_GSTR2
    #                 update_query = """
    #                 UPDATE {table_name}
    #                 SET Status = :program_status
    #                 WHERE 
    #                     Status IS NULL OR Status = '' OR Status = 'FAILED'
    #                 """
    #
    #                 for table in ['ClearTax_GSTR1', 'ClearTax_GSTR2']:
    #                     try:
    #                         cursor.execute(update_query.format(table_name=table), {
    #                             'program_status': program_status,
    #                         })
    #                         conn.commit()
    #                         print(f"Program status updated for '{file_name}' in table '{table}' successfully.")
    #                     except Exception as e:
    #                         logging.error(f"Error updating table '{table}': {e}")
    #
    #     except Exception as e:
    #         logging.error(f"An error occurred while inserting audit log: {e}")
    #
    #     finally:
    #         # No connection management needed here
    #         pass
        # ----------------------------------------------------------------------------------
    # def insert_audit_log(
#         program_type, file_name, get_presigned_url_status, upload_file_status,
#         activity_id, trigger_file_ingestion_status, get_file_ingestion_status,
#         start_time, end_time, error_message, program_status,conn
    #     ):
    #     try:
            
    #         log_id = str(uuid.uuid4())
            

    #         # Define the SQL query to insert a new record into the audit log
    #         query = """
    #         INSERT INTO AuditLogTable (
    #             UUID, ProgramType, FileName, GetPresignedURLAPIStatus, 
    #             UploadFileToStorageAPIStatus, Activity_id, TriggerFileIngestionAPIStatus, 
    #             GetFileIngestionStatus, StartTime, EndTime, ErrorMessage, ProgramStatus
    #         ) VALUES (
    #             :log_id, :program_type, :file_name, :get_presigned_url_status, 
    #             :upload_file_status, :activity_id, :trigger_file_ingestion_status, 
    #             :get_file_ingestion_status, TO_DATE(:start_time, 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(:end_time, 'YYYY-MM-DD HH24:MI:SS'), :error_message, :program_status
    #         )
    #         """

    #         # Define the parameters for the SQL query
    #         params = {
    #             'log_id': log_id,
    #             'program_type': program_type,
    #             'file_name': file_name,
    #             'get_presigned_url_status': get_presigned_url_status,
    #             'upload_file_status': upload_file_status,
    #             'activity_id': activity_id,
    #             'trigger_file_ingestion_status': trigger_file_ingestion_status,
    #             'get_file_ingestion_status': get_file_ingestion_status,
    #             'start_time': start_time,
    #             'end_time': end_time,
    #             'error_message': error_message,
    #             'program_status': program_status
    #         }

    #         # Execute the SQL query
    #         with conn.cursor() as cursor:
    #             cursor.execute(query, params)
    #             conn.commit()
    #         print(f"Audit log for file '{file_name}' inserted successfully")

            # Update ProgramStatus in ClearTax_GSTR1 where it is 'FAILED' or empty
            # update_gstr1 = """
            # UPDATE ClearTax_GSTR1
            # SET Status = :program_status
            # WHERE Status = 'FAILED' OR Status IS NULL OR Status = ''
            # """

            # # Update ProgramStatus in ClearTax_GSTR2 where it is 'FAILED' or empty
            # update_gstr2 = """
            # UPDATE ClearTax_GSTR2
            # SET Status = :program_status
            # WHERE Status = 'FAILED' OR Status IS NULL OR Status = ''
            # """

            # # Execute the update for ClearTax_GSTR1
            # cursor.execute(update_gstr1, {
            #     'program_status': program_status,
            # })

            # # Execute the update for ClearTax_GSTR2
            # cursor.execute(update_gstr2, {
            #     'program_status': program_status,
            # })

            # # Commit all changes
            # conn.commit()

            # print(f"Audit log and program status updated for file '{file_name}' successfully.")
        # ----------------------------------------------------------------
            # print(f"Audit log for file '{file_name}' inserted successfully")

        # except Exception as e:
        #     print(f"An error occurred while inserting audit log: {e}")

        # finally:
        #     # Ensure the connection is closed
        #     if conn:
        #         conn.close()

