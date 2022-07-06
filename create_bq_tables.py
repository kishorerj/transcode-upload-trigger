import argparse,os, json, logging
from google.cloud import bigquery


logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

logger = logging.getLogger("create_bq_tables")
bigquery_client = bigquery.Client()


TRANSCODER_TABLE = [
    bigquery.SchemaField("job_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("start_date", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("end_date", "DATETIME", mode="NULLABLE"),
     bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
      bigquery.SchemaField("input_media_uri", "STRING", mode="NULLABLE"),
       bigquery.SchemaField("output_transcoded_uri", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("job_template", "STRING", mode="NULLABLE"),
     bigquery.SchemaField("error_msg", "STRING", mode="NULLABLE"),

]
def create_transcode_table():
    dataset_name="transcode_media"
    table_name="transcoder_job_dtls"
    dataset_ref=bigquery.dataset.DatasetReference("media-348707",
                                             dataset_name)
 
    table_ref = bigquery.table.TableReference(dataset_ref, table_name)

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "us-east1"
    dataset = bigquery_client.create_dataset(dataset_ref, exists_ok=True)

    logger.info("Dataset created")

    table = bigquery.Table(table_ref, schema=TRANSCODER_TABLE)

    bigquery_client.create_table(table, exists_ok=True)

if __name__ == "__main__":
    create_transcode_table()