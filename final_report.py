import configparser
from google.cloud import bigquery
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
except Exception as e:
    raise Exception("Client Error {}".format(e))

def query_to_df(query):
    try:
        cleared_data = []
        query_job = bigquery_client.query(query)
    except Exception as e:
        raise ("Something happened while query was running", e)

# Most popular 3 pickups, dropoffs, and routes
query = """
CREATE OR REPLACE TABLE `teknasyon-340116.taxi_ds.final_taxi_report`  AS
SELECT 
pickup.rnt as rank,
pickup.pickup_hexagon as most_popular_pickup_hexagons, pickup.pickup_hexagon_popularity,
dropoff.dropoff_hexagon as most_popular_dropoff_hexagons, dropoff.dropoff_hexagon_popularity,
route_t.route as most_popular_routes, route_t.route_popularity	 
FROM (
SELECT *, ROW_NUMBER() OVER() as rnt 
FROM (
    SELECT pickup_hexagon, COUNT(*) AS pickup_hexagon_popularity, 
    FROM `teknasyon-340116.taxi_ds.adjusted_taxi_data` 
    GROUP BY 1 ORDER BY pickup_hexagon_popularity DESC LIMIT 5
    ) 
)pickup
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER() as rnt FROM (
        SELECT dropoff_hexagon, COUNT(*) AS dropoff_hexagon_popularity
        FROM `teknasyon-340116.taxi_ds.adjusted_taxi_data` 
        GROUP BY 1 ORDER BY dropoff_hexagon_popularity DESC LIMIT 5
    )
)dropoff on pickup.rnt=dropoff.rnt
LEFT JOIN
(   SELECT *, ROW_NUMBER() OVER() as rnt FROM (
        SELECT route, COUNT(*) AS route_popularity FROM (
        SELECT *, CONCAT(pickup_hexagon, '-', dropoff_hexagon) AS route FROM `teknasyon-340116.taxi_ds.adjusted_taxi_data` 
        ) GROUP BY 1 ORDER BY route_popularity desc LIMIT 5
    )
)route_t ON route_t.rnt=dropoff.rnt
ORDER BY rank asc
"""
data = query_to_df(query)