import configparser
from google.cloud import bigquery
from google.cloud import storage
import os

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


def query_to_df(query):
    try:
        cleared_data = []
        query_job = bigquery_client.query(query)
    except Exception as e:
        raise ("Something happened while query was running", e)

# Cleaning Query
query = f"""
CREATE OR REPLACE TABLE  `teknasyon-340116.taxi_ds.clean_taxi_data` AS
SELECT pickup_datetime, dropoff_datetime, store_and_fwd_flag, rate_code, pickup_longitude, pickup_latitude,
        dropoff_longitude, dropoff_latitude, passenger_count, trip_distance, 
        fare_amount, extra, mta_tax, tip_amount, tolls_amount, total_amount, payment_type
FROM `teknasyon-340116.taxi_ds.taxi_*` 
WHERE (pickup_longitude != 0 or pickup_latitude != 0 or dropoff_longitude != 0 or dropoff_latitude != 0) # Coordinates should not be 0. GPS readings can be wrong
AND passenger_count > 0 # I assume that should be passengers if we want accept route as a service 
AND trip_distance > 0 # The trip distance of the taxi must be greater than 0 per route because if the distance is not traveled, the system can be manipulated and this can be mislead the popularity of coordinates.
"""
data = query_to_df(query)
