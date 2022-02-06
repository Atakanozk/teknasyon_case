import configparser
from google.cloud import bigquery
from google.cloud import storage
import os
from google.cloud.exceptions import Conflict
import pandas as pd
from datetime import datetime, timedelta, date
import sys
try:
    machine = sys.argv[1]
except Exception as e:
    raise('enter machine type', e)

config = configparser.ConfigParser()
config.read("config.ini")
try:
    cred_file = config.get('gcp', 'cred_file')
    cwd = os.path.dirname(os.path.realpath(__file__))
    if machine == 'windows':
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}\{}".format(cwd,
                                                                  cred_file)
    elif machine == 'linux':
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}/{}".format(cwd,
                                                                  cred_file)

except Exception as e:
    raise Exception(""
                    "Creds can not be loaded")

try:
    bigquery_client = bigquery.Client()
    storage_client = storage.Client()
except Exception as e:
    raise Exception("Client Error {}".format(e))




# Checking if csvs in cloud storage exists if not create them
table_names = []
for blob in storage_client.list_blobs('taxib', prefix='first_week/taxi'):
    table_names.append((str(blob.name).split('/', 1)[1]).split('.', 1)[0])

if table_names:
    print(f"CSVs already exists", table_names)
    sys.exit()

def query_to_df(query):
    try:
        query_job = bigquery_client.query(query)
        rows_df = query_job.result().to_dataframe()

        return rows_df
    except Exception as e:
        raise ("Something happened while query was running", e)


public_to_cs_query = f"""SELECT * 
FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014` 
WHERE DATE(pickup_datetime) BETWEEN DATE('2014-03-01') and DATE('2014-03-07')"""
data = query_to_df(public_to_cs_query)

for i in range(7):
    start_date = '2014-03-01'
    one_day_later = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    if i != 0:
        start_date = (datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=i)).strftime('%Y-%m-%d')
        one_day_later = (datetime.strptime(one_day_later, '%Y-%m-%d') + timedelta(days=i)).strftime('%Y-%m-%d')
    print(i, ') ', start_date, '---', one_day_later)
    deep_copied_data = data.copy()
    one_day_data = deep_copied_data.loc[(deep_copied_data['pickup_datetime'] >= start_date)
                                        & (deep_copied_data['pickup_datetime'] < one_day_later)]

    bucket_name = 'taxib'  # Bucket Name
    bucket = storage_client.get_bucket(bucket_name)  # change the bucket name
    blob = bucket.blob('first_week/taxi_{}.csv'.format(start_date))
    blob.upload_from_string(one_day_data.to_csv(sep=';', index=False, encoding='utf-8'),
                            content_type='application/octet-stream')
