import os
import urllib.request as urllib2
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow_custom_operators.s3_file_transfer import S3FileTransferOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_custom_operators.create_drop_tbl import CreateDropTableRedshiftOperator
from airflow_custom_operators.stage_redshift import StageToRedshiftOperator
from airflow_custom_operators.data_quality import DataQualityOperator

from helper.emr_operators_configuration import EmrOperatorsConfiguration
from zipfile import ZipFile

# Define file path here
current_dir = os.path.dirname(__file__)
create_sql = "sql/create_tables.sql"
drop_sql = "sql/drop_tables.sql"
fd1 = open(os.path.join(current_dir, create_sql), 'r')
fd2 = open(os.path.join(current_dir, drop_sql), 'r')
CREATE_SQL = fd1.read()
DROP_SQL = fd2.read()

DEFAULT_DATA_PATH = os.path.join(current_dir, "bikeshare_nyc/zipped_data/")
DEFAULT_UNZIPPED_DATA_PATH = os.path.join(current_dir, "bikeshare_nyc/unzipped_data/")
WEATHER_FILE = os.path.join(current_dir, "bikeshare_nyc/weather_data/")
SCRIPT_DATA = os.path.join(current_dir, "bikeshare_nyc/etl_script/")

IAM_ROLE = "arn:aws:iam::507029168794:role/thuannt-Redshift-role"

#Define S3 name here:
BUCKET_NAME = "nyc-bikeshare-trip-data"
TRIP_DATA_FOLDER = "citibike-tripdata"
WEATHER_DATA_FOLDER = "weather-data"
SCRIPT_FOLDER = "etl-script"
#Transformed dim and fact table s3 key
TRANSFORMED_TABLE_FOLDER = "transformed-table"

list_urls = ["https://s3.amazonaws.com/tripdata/202001-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202002-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202003-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202004-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202005-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202006-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202007-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202008-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202009-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202010-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202011-citibike-tripdata.csv.zip",
                 "https://s3.amazonaws.com/tripdata/202012-citibike-tripdata.csv.zip"
             ]

def unzip_file(**kwargs):
    """
       Loop through dir_name folder and check for zipped files with extension,
       extract all of them to output and remove zipped file
       :param dir_name: string directory path of zipped data
       :param extension: string file extension
       :param output: location to store unzipped files
   """
    if not os.listdir(kwargs['dir_name']):
        for item in os.listdir(kwargs['dir_name']):  # loop through items in dir
            if item.endswith(kwargs['extension']):  # check for ".zip" extension
                file_name = os.path.abspath(item)  # get full path of files
                print(file_name)
                with ZipFile(file_name, 'r') as zipObj:
                    # Extract all the contents of zip file in current directory
                    zipObj.extractall(kwargs['output'])
                    zipObj.close()  # close file
                os.remove(file_name)  # delete zipped file


def downloader(url, output):
    """
       Download the Citi Bike Station Data.
       :param url: string url of the data file
       :param path: string - path to save the file
       """
    if not os.listdir(output):
        file_name = url.split("/")[-1]
        outfile = open(output + file_name, 'wb')
        tmp_data = urllib2.urlopen(url).read()
        outfile.write(tmp_data)
        outfile.close()


def download_from_urls(**kwargs):
    for url in kwargs['urls']:
        downloader(url, kwargs['output'])

# Default arguments with prepared setting
default_args = {
    'owner': 'thuannt.se',
    'depends_on_past': False,
    'email': ['thuannt.se@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'nyc_bikeshare_datawarehouse',
    default_args=default_args,
    description="Simple dag to create New York Citty citybike datawarehouse",
    schedule_interval=timedelta(days=30),
    start_date=days_ago(2)
) as dag:
    start = DummyOperator(
        task_id="start_etl"
    )
    download_from_s3 = PythonOperator(
        task_id="get_data_from_citybike",
        python_callable=download_from_urls,
        op_kwargs={'urls': list_urls, 'output': DEFAULT_UNZIPPED_DATA_PATH}
    )

    unzip_data = PythonOperator(
        task_id="unzip_data_before_upload",
        python_callable=unzip_file,
        op_kwargs={"dir_name": DEFAULT_DATA_PATH, "extension": ".zip", "output": DEFAULT_UNZIPPED_DATA_PATH}
    )

    upload_unzipped_to_s3 = S3FileTransferOperator(
        task_id="upload_data_to_datalak",
        operation="UPLOAD",
        aws_conn_id="aws_default",
        s3_bucket=BUCKET_NAME,
        s3_key=TRIP_DATA_FOLDER,
        local_file_path=DEFAULT_UNZIPPED_DATA_PATH
    )

    upload_weather_data_to_s3 = S3FileTransferOperator(
        task_id="upload_weather_data_to_datalake",
        operation="UPLOAD",
        aws_conn_id="aws_default",
        s3_bucket=BUCKET_NAME,
        s3_key=WEATHER_DATA_FOLDER,
        local_file_path=WEATHER_FILE
    )

    upload_etl_script_to_s3 = S3FileTransferOperator(
        task_id="upload_etl_script_to_s3",
        operation="UPLOAD",
        aws_conn_id="aws_default",
        s3_bucket=BUCKET_NAME,
        s3_key=SCRIPT_FOLDER,
        local_file_path=SCRIPT_DATA
    )

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EmrOperatorsConfiguration.JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_default",
        emr_conn_id="emr_default",
        params={  # these params are used to fill the paramterized values in SPARK_STEPS json
            "s3_input_bucket_name": BUCKET_NAME,
            "s3_script_folder": SCRIPT_FOLDER
        }
    )

    emr_step_execute_script = EmrAddStepsOperator(
        task_id="execute_script",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=EmrOperatorsConfiguration.SPARK_STEPS,
        params={  # these params are used to fill the paramterized values in SPARK_STEPS json
            "s3_input_bucket_name": BUCKET_NAME,
            "s3_script_folder": SCRIPT_FOLDER,
            "s3_script": "etl.py",
            "s3_trip_data_folder": TRIP_DATA_FOLDER,
            "transformed_table": TRANSFORMED_TABLE_FOLDER,
            "year": "2020"
        }
    )

    last_step = len(EmrOperatorsConfiguration.SPARK_STEPS) - 1  # this value will let the sensor know the last step to watch
    # wait for the steps to complete
    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='execute_script', key='return_value')["
                + str(last_step)
                + "] }}",
        aws_conn_id="aws_default"
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default"
    )

    create_table_redshift = PostgresOperator(
        task_id='create_drop_redshift_table',
        postgres_conn_id="redshift",
        sql=DROP_SQL + CREATE_SQL
    )

    stage_weather_fact_to_redshift = StageToRedshiftOperator(
        task_id='copy_weather_fact_table',
        redshift_conn_id="redshift",
        table="weather_fact",
        s3_bucket_id=BUCKET_NAME,
        s3_key=TRANSFORMED_TABLE_FOLDER + "/2020/weather-fact-table/",
        iam_role=IAM_ROLE,
    )

    stage_weather_type_to_redshift = StageToRedshiftOperator(
        task_id='copy_weather_type_data',
        redshift_conn_id="redshift",
        table="weather_type",
        s3_bucket_id=BUCKET_NAME,
        s3_key=TRANSFORMED_TABLE_FOLDER + "/2020/weather-type-table/",
        iam_role=IAM_ROLE,
    )

    stage_weather_type_relation_to_redshift = StageToRedshiftOperator(
        task_id='copy_weather_type_relation_data',
        redshift_conn_id="redshift",
        table="date_with_weather_type",
        s3_bucket_id=BUCKET_NAME,
        s3_key=TRANSFORMED_TABLE_FOLDER + "/2020/dim-datetime-weather-table/",
        iam_role=IAM_ROLE,
    )

    stage_trip_fact_to_redshift = StageToRedshiftOperator(
        task_id='copy_tripdata_fact',
        redshift_conn_id="redshift",
        table="trip_fact",
        s3_bucket_id=BUCKET_NAME,
        s3_key=TRANSFORMED_TABLE_FOLDER + "/2020/tripfact-table/",
        iam_role=IAM_ROLE,
    )

    stage_dim_station_to_redshift = StageToRedshiftOperator(
        task_id='copy_dim_station',
        redshift_conn_id="redshift",
        table="dim_station",
        s3_bucket_id=BUCKET_NAME,
        s3_key=TRANSFORMED_TABLE_FOLDER + "/2020/dim-station-table/",
        iam_role=IAM_ROLE,
    )

    stage_dim_datetime_to_redshift = StageToRedshiftOperator(
        task_id='copy_dim_datetime',
        redshift_conn_id="redshift",
        table="dim_datetime",
        s3_bucket_id=BUCKET_NAME,
        s3_key=TRANSFORMED_TABLE_FOLDER + "/2020/dim-datetime-table/",
        iam_role=IAM_ROLE,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        table="trip_fact",
        table_id="trip_id",
    )
    run_quality_weather_checks = DataQualityOperator(
        task_id='Run_weather_data_quality_checks',
        redshift_conn_id="redshift",
        table="weather_fact",
        table_id="date_time",
    )

    start >> download_from_s3 >> unzip_data >> upload_unzipped_to_s3 >> upload_etl_script_to_s3
    start >> upload_weather_data_to_s3 >> upload_etl_script_to_s3
    upload_etl_script_to_s3 >> create_emr_cluster >> emr_step_execute_script >> step_checker >> terminate_emr_cluster

    terminate_emr_cluster >> create_table_redshift >> [stage_weather_fact_to_redshift, stage_trip_fact_to_redshift]

    stage_weather_fact_to_redshift >> [stage_weather_type_to_redshift, stage_weather_type_relation_to_redshift] >> run_quality_weather_checks
    stage_trip_fact_to_redshift >> [stage_dim_station_to_redshift, stage_dim_datetime_to_redshift] >> run_quality_checks

