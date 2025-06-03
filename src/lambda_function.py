import psycopg2
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import json
import os
import base64

def lambda_handler(event, context):
    try:
        # Get Redshift credentials from environment variables
        redshift_host = os.environ['REDSHIFT_HOST']
        redshift_database = os.environ['REDSHIFT_DATABASE']
        redshift_user = os.environ['REDSHIFT_USER']
        redshift_password = os.environ['REDSHIFT_PASSWORD']
        
        # Get Google Sheets credentials (base64 encoded)
        encoded_creds = os.environ['GOOGLE_CREDS_BASE64']
        decoded_creds = base64.b64decode(encoded_creds).decode('utf-8')
        google_creds = json.loads(decoded_creds)
        
        # Get parameters from Lambda event
        query = event.get('query')
        spreadsheet_id = event.get('spreadsheet_id')
        sheet_name = event.get('sheet_name', 'Sheet1')
        
        if not all([query, spreadsheet_id]):
            raise ValueError("Missing required parameters: query and spreadsheet_id")
        
        # Connect to Redshift
        redshift_conn = connect_to_redshift(
            host=redshift_host,
            database=redshift_database,
            user=redshift_user,
            password=redshift_password
        )
        
        if not redshift_conn:
            raise Exception("Failed to connect to Redshift")
        
        # Execute query
        df = execute_redshift_query(redshift_conn, query)
        redshift_conn.close()
        
        if df is None or df.empty:
            return {'statusCode': 200, 'body': 'No data to transfer'}
        
        # Set up Google Sheets connection
        sheets_service = get_google_sheets_service_from_json(google_creds)
        if not sheets_service:
            raise Exception("Failed to create Google Sheets service")
        
        # Write data to Google Sheets
        success = write_to_google_sheets(
            service=sheets_service,
            spreadsheet_id=spreadsheet_id,
            sheet_name=sheet_name,
            df=df
        )
        
        if not success:
            raise Exception("Failed to write data to Google Sheets")
        
        return {'statusCode': 200, 'body': f'Successfully transferred {len(df)} rows'}
        
    except Exception as e:
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}


def connect_to_redshift(host, database, user, password, port=5439):
    """Connect to Amazon Redshift cluster using psycopg2"""
    try:
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        print("Successfully connected to Redshift!")
        return conn
    except Exception as e:
        print(f"Error connecting to Redshift: {e}")
        return None

def get_google_sheets_service_from_json(credentials_json):
    """Authenticate with Google Sheets API using service account JSON"""
    try:
        creds = service_account.Credentials.from_service_account_info(
            credentials_json,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        service = build('sheets', 'v4', credentials=creds)
        print("Google Sheets API service created successfully!")
        return service
    except Exception as e:
        print(f"Error creating Google Sheets service: {e}")
        return None

def execute_redshift_query(conn, query):
    """Execute a SQL query on Redshift and return results as a DataFrame"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()

        df = pd.DataFrame(data, columns=columns)
        print(f"Query executed successfully. Returned {len(df)} rows.")
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        cursor.close()

def write_to_google_sheets(service, spreadsheet_id, sheet_name, df, starting_cell='A1'):
    """Write a Pandas DataFrame to Google Sheets"""
    try:
        # Convert datetime columns to strings
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Prepare data for Google Sheets API
        data = [df.columns.values.tolist()] + df.values.tolist()
        range_name = f"{sheet_name}!{starting_cell}"

        body = {'values': data}

        # Update the sheet
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()

        print(f"Successfully wrote {len(df)} rows to Google Sheets.")
        print(f"Updated {result.get('updatedCells')} cells.")
        return True
    except Exception as e:
        print(f"Error writing to Google Sheets: {e}")
        return False                        

