# [START first-composer]
import datetime
import airflow
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators import dataproc_operator

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Composer Demo',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': yesterday,
}

gcs_output_bucket = 'myworkspace'
bq_dataset_name = 'mydataset'
bq_table_id = bq_dataset_name + '.my_table_name'

with airflow.DAG(
        'composer_sample_dag',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:

    Spark_Job = dataproc_operator.DataprocWorkflowTemplateInstantiateOperator(
        task_id='spark_job',
        template_id='mytemplate',
        project_id='my-project-id',
        dag=dag)

    GCS_to_BQ = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='GCS_to_BQ',
        bucket=gcs_output_bucket,
        source_objects=['output/*.parquet'],
        destination_project_dataset_table=bq_table_id,
        source_format='PARQUET',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

    Spark_Job >> GCS_to_BQ
# [END composer_quickstart]
