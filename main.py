from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
import sys
import pandas as pd
import json
import re 
import numpy as np
import time

def send_bq_logs(df):
    schema = [
    bigquery.SchemaField("operator", "string"),
    bigquery.SchemaField("user_id", "string"),
    bigquery.SchemaField("pass_or_fail", "string"),
    bigquery.SchemaField("mac_address", "string"),
    bigquery.SchemaField("uuid", "string"),
    bigquery.SchemaField("operating_system", "string"),
    bigquery.SchemaField("transaction_id", "string"),
    bigquery.SchemaField("time_utc", "string"),
    bigquery.SchemaField("solution", "string"),
    bigquery.SchemaField("reason", "string"),
    bigquery.SchemaField("ip_address", "string"),
    bigquery.SchemaField("isp", "string"),
    bigquery.SchemaField("primary_source", "string"),
    bigquery.SchemaField("secondary_source", "string"),
    bigquery.SchemaField("country_code", "string"),
    bigquery.SchemaField("region_code", "string"),
    bigquery.SchemaField("secondary_country_code", "string"),
    bigquery.SchemaField("secondary_region_code", "string"),
    bigquery.SchemaField("latitude", "string"),
    bigquery.SchemaField("longitude", "string"),
    bigquery.SchemaField("accuracy", "string"),
    bigquery.SchemaField("reason_for_failure", "string"),
    bigquery.SchemaField("error_message", "string"),
    bigquery.SchemaField("troubleshooter", "string"),
    bigquery.SchemaField("transaction_log", "string"),
    ]
    credentials = service_account.Credentials.from_service_account_file(
    "keyfile.json", scopes=["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition = 'WRITE_APPEND'
    )

    job = client.load_table_from_dataframe(df,"cg-maximbet-bi.stage.geocomply_temporary", job_config=job_config)

def call_merge():
    cred = service_account.Credentials.from_service_account_file("keyfile.json")

    client = bigquery.Client(credentials= cred,project="cg-maximbet-bi")
    query = """
       CALL stage.geocomply()
            """
    try:
        job = client.query(query=query)
        print('Merging results in DataModel...')
        job.result() # Wating the completition of the Job
        print("This procedure processed {} bytes.".format(job.total_bytes_processed))
        print(job.total_bytes_processed)
    except Exception as err:
        print(f'An error occurred: {err}')


credentials = service_account.Credentials.from_service_account_file("keybucket.json")
storage_client = storage.Client(credentials=credentials)
bucket = storage_client.get_bucket("geocomply")
blobs = bucket.list_blobs()
blobs = list(blobs)

lists = ["operator","user_id","pass_or_fail","mac_address","uuid","operating_system","transaction_id","time_utc","solution","reason","ip_address","isp","primary_source","secondary_source","country_code","region_code",
"secondary_country_code", "secondary_region_code","latitude","longitude","accuracy","reason_for_failure", "error_message","troubleshooter","transaction_log"]

bloobs = []
for blob in blobs:
    try:
        time.sleep(1)
        print("blooping")
        bloobs.append(blob.name)
    except Exception as err:
        print(f'An error occurred: {err}')
        
def call_buckets_merge(bucket_name):
    try:
        name = '_'.join(re.findall('[0-9]+', str(bucket_name))[:2])
        blob = bucket.blob(str(bucket_name))
        downloaded_blob = blob.download_as_string()
        downloaded_blob = downloaded_blob.decode("utf-8") 
        df = pd.DataFrame(json.loads(downloaded_blob))
        if df.size != 0:
            df.insert(len(df.columns),"transaction_log",name)
            if list(set(lists) - set(df.columns)) != []:
                for i in list(set(lists) - set(df.columns)):
                    df.insert(len(df.columns),str(i),0)
                    print("column inserted")
            else:
                pass
            df = df[["operator","user_id","pass_or_fail","mac_address","uuid","operating_system","transaction_id","time_utc","solution","reason","ip_address","isp",
                     "primary_source","secondary_source","country_code","region_code","secondary_country_code", "secondary_region_code","latitude","longitude","accuracy",
                     "reason_for_failure", "error_message","troubleshooter","transaction_log"]]
            
            df = df.replace('',0)
            df = df.astype(str)
            df.accuracy = df.accuracy.astype(str)
            df.latitude = df.latitude.astype(str)
            df.longitude = df.longitude.astype(str)
            send_bq_logs(df)
            print("sending bq logs",name)
            call_merge()
        else:
            print("pass",blob.name)
        
        
    except:
        print("Oops!", sys.exc_info(), "occurred.")
        print("as no values",blob.name,)
        

for i in bloobs[-2:]:
    call_buckets_merge(i)



