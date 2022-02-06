import configparser
from google.cloud import bigquery
from google.cloud import storage
import os
from google.cloud.exceptions import NotFound
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


def extract_schema(client):
    """

    :param client: bigquery client
    :return: schema of public ds
    """
    project = 'bigquery-public-data'
    dataset_id = 'new_york_taxi_trips'
    table_id = 'tlc_green_trips_2014'

    dataset_ref = bigquery_client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)  # API Request

    return [bigquery.SchemaField(schema.name, 'FLOAT64' if schema.field_type == 'INTEGER' else schema.field_type) for schema in table.schema]

try:
    schema = extract_schema(bigquery_client)
except Exception as e:
    raise("Schema Could not pulled", e)

# Pulling names of csv files in order to create same name tables in bigquery
table_names = []
for blob in storage_client.list_blobs('taxib', prefix='first_week/taxi'):
    table_names.append((str(blob.name).split('/', 1)[1]).split('.', 1)[0])


job_config = bigquery.LoadJobConfig(
    schema=schema,
    skip_leading_rows=1,
    allow_jagged_rows=True,
    source_format=bigquery.SourceFormat.CSV,
    field_delimiter=";"
)

for table_name in table_names:
    uri = "gs://taxib/first_week/{}.csv".format(table_name)
    table_id = "teknasyon-340116.taxi_ds.{}".format(table_name.replace('-',''))

    try:
        bigquery_client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        print("Deleting table")
        bigquery_client.delete_table(table_id, not_found_ok=True)
    except NotFound:
        print("Table {} is not found.".format(table_id))

    load_job = bigquery_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    ) # Request for creating bq table with desired schema and push data into tables

    load_job.result()

    destination_table = bigquery_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
