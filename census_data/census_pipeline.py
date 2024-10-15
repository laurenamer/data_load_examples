import dlt
from dlt.sources.rest_api import rest_api_source

pipeline = dlt.pipeline(
    pipeline_name='census',
    destination="bigquery", 
    dataset_name="census_data"
)

info = pipeline.run([{'id':1}, {'id':2}, {'id':3}], table_name="three")

print(info)