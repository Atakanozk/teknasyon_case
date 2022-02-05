import configparser
from google.cloud import bigquery
from google.cloud import storage
import os
import h3
from google.cloud.exceptions import NotFound
import time


config = configparser.ConfigParser()
config.read("config.ini")
try:
    cred_file = config.get('gcp', 'cred_file')
    cwd = os.path.dirname(os.path.realpath(__file__))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "{}\{}".format(cwd,
                                                                  cred_file)

except Exception as e:
    raise Exception(""
                    "Creds can not be loaded")

try:
    bigquery_client = bigquery.Client()
    storage_client = storage.Client()
except Exception as e:
    raise Exception("Client Error {}".format(e))


query = f"""
SELECT *, CASE 
            WHEN TIME(pickup_datetime) BETWEEN CAST('00:00:00' AS TIME)  AND  CAST('05:59:59'AS TIME) THEN 'Night'
            WHEN TIME(pickup_datetime) BETWEEN CAST('06:00:00' AS TIME)  AND  CAST('11:59:59'AS TIME) THEN 'Morning'
            WHEN TIME(pickup_datetime) BETWEEN CAST('12:00:00' AS TIME)  AND  CAST('17:59:59'AS TIME) THEN 'Noon'
            WHEN TIME(pickup_datetime) BETWEEN CAST('18:00:00' AS TIME)  AND  CAST('23:59:59'AS TIME) THEN 'Evening'
 END AS daypart  FROM `teknasyon-340116.taxi_ds.clean_taxi_data` 
"""
try:
    cleared_data = []
    query_job = bigquery_client.query(query)
    results = query_job.result()
    for i in results:
        cleared_data.append([i[0], i[1], i[2], i[3],
                            i[4], i[5], i[6], i[7],
                            i[8], i[9] ,i[10], i[11],
                            i[12], i[13], i[14], i[15],
                            i[16], i[17]
                        ])
except Exception as e:
    raise ("Something happened while query was running", e)


for d in cleared_data:
    d.append(h3.geo_to_h3(d[5],  # pickup_latitude
                          d[4],  # pickup_longitude
                          9))

    d.append(h3.geo_to_h3(d[7],  # dropoff_latitude
                          d[6],  # dropoff_longitude
                          9))


def extract_schema(client):
    """

    :param client: bigquery client
    :return: schema of public ds
    """
    project = 'teknasyon-340116'
    dataset_id = 'taxi_ds'
    table_id = 'clean_taxi_data'

    dataset_ref = bigquery_client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)  # API Request

    return [bigquery.SchemaField(schema.name, 'FLOAT64' if schema.field_type == 'INTEGER' else schema.field_type) for schema in table.schema]


try:
    schema = extract_schema(bigquery_client)
    schema.extend([bigquery.SchemaField('daypart', 'STRING'),
                   bigquery.SchemaField('pickup_hexagon', 'STRING'),
                   bigquery.SchemaField('dropoff_hexagon', 'STRING')])
except Exception as e:
    raise("Schema Could not pulled", e)

project_name = 'teknasyon-340116'
dataset_name = 'taxi_ds'
dataset_ref = bigquery_client.dataset('{}'.format(dataset_name))
table_name = 'adjusted_taxi_data'
table_ref = dataset_ref.table(table_name)

try:
    bigquery_client.get_table(table_ref)  # Make an API request.
    print("Table {} already exists.".format(table_ref))
    print("Deleting table")
    bigquery_client.delete_table(table_ref, not_found_ok=True)
except NotFound:
    print("Table {} is not found.".format(table_ref))

table = bigquery.Table(table_ref, schema=schema)
table = bigquery_client.create_table(table)
table = bigquery_client.get_table(table_ref)

split_data = [cleared_data[i:i + 10000] for i in
              range(0, len(cleared_data), 10000)]
loop = True
while loop:
    try:
        for s in split_data:
            result = bigquery_client.insert_rows(table, s)
            print(result, s.index())
        loop = False
    except NotFound:
        print('Not Found trying again')
        time.sleep(2)
    except Exception as e:
        print('Unexpected error', e)