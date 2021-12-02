from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator


s3_key_path = ["sensor_demo.csv"]
s3_uri = 's3://s3-sensor-demo/airflow-sensor-demo/sensor_demo.csv'

# query for creating a snowflake external stage
#-----------------------------------------------------------------------------------
# """
# CREATE or REPLACE STAGE <NAME_OF_THE_EXTERNAL_STAGE>
# URL=<S3_BUCKET_KEY>
# CREDENTIALS=(AWS_KEY_ID=<AWS_KEY_ID> AWS_SECRET_KEY=<AWS_SECRET_KEY>);
# """
#-----------------------------------------------------------------------------------

query_table_init = [
    """DROP TABLE IF EXISTS employee_info""",

    """CREATE TABLE employee_info (
        employee_id VARCHAR PRIMARY KEY, 
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        occupation VARCHAR NOT NULL,
        department VARCHAR NOT NULL
        );""",

    """INSERT INTO employee_info (employee_id, first_name, last_name, occupation, department) 
        VALUES ('A00030','Stephen','Curry','Full Stack Developer','Technology' );"""
]


query_find_GOAT = "select * from employee_info where last_name = 'Jordan';"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(dag_id='sensor_demo',
    default_args=default_args,
    description='sensor demo',
    start_date=datetime(2021,11,30),
    catchup=False,
    tags=['sensor_demo'])

def sensor_demo():
    table_init = SnowflakeOperator(
        task_id="table_init",
        sql=query_table_init,
        snowflake_conn_id="snowflake_connection",
    )

    s3_file_sensor = S3KeySensor(
        task_id='s3_key_sensor',
        mode='poke',
        poke_interval=10,
        timeout=60*5,
        bucket_key=s3_uri,
        bucket_name=None,
        aws_conn_id="s3_connection",
        wildcard_match=False
    )

    copy_into_table = S3ToSnowflakeOperator(
        task_id='s3_to_snowflake',
        s3_keys=s3_key_path,
        snowflake_conn_id="snowflake_connection",
        table="employee_info",
        schema="public",
        stage="employee_info_external_stage",
        file_format="(type = 'CSV',field_delimiter = ',')"
    )


    find_GOAT_sensor = SqlSensor(
        task_id='new_records_sensor',
        conn_id='snowflake_connection',
        sql=query_find_GOAT,
        mode='poke',
        poke_interval=10,
        timeout=60*5,
    )

    def find_GOAT(**context):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_connection")
        goat_df = hook.get_pandas_df("select * from employee_info where last_name = 'Jordan'")
        print(goat_df)

    find_GOAT = PythonOperator(
        task_id='find_GOAT',
        python_callable=find_GOAT,
    )

    table_init >> s3_file_sensor >> copy_into_table >> find_GOAT_sensor >> find_GOAT

dag = sensor_demo()