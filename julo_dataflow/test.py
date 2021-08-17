from google.cloud import bigquery
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
credentials = GoogleCredentials.get_application_default()
service = discovery.build('compute', 'v1', credentials=credentials)

client = bigquery.Client()
dataset_id = 'sushant_julo'

dataset_ref = client.dataset(dataset_id)

job_config = bigquery.LoadJobConfig()

job_config.schema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("email", "STRING"),
]

job_config.skip_leading_rows = 1
# The source format defaults to CSV, so the line below is optional.
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
uri = "gs://sushant-julo-test-blucket/topics/dbserver1.inventory.customers/partition=0/dbserver1.inventory.customers+0+0000000000.json"

load_job = client.load_table_from_uri(
    uri, dataset_ref.table("demo"), job_config=job_config
)  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
print("Job finished.")

destination_table = client.get_table(dataset_ref.table("people"))
print("Loaded {} rows.".format(destination_table.num_rows))
