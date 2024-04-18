from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import os
import shutil
from datetime import datetime
import pandas as pd
import numpy as np
import psycopg2
import uuid
from sqlalchemy import create_engine


def add_executions(**kwargs):
    conn_string = 'postgresql://postgres:postgres@host.docker.internal:5432/ib_trade_db'
    db = create_engine(conn_string)

    landing_directory = '/opt/airflow/container_home/trade_executions/landing'
    processed_directory = '/opt/airflow/container_home/trade_executions/processed'

    dir_list = os.listdir(landing_directory)
    executions_file = dir_list[0]
    executions_filepath = os.path.join(landing_directory, executions_file)

    executions_df = pd.read_csv(executions_filepath)

    # Move processed file from landing directory
    processed_filepath = os.path.join(processed_directory, executions_file)
    try:
        shutil.copy(executions_filepath, processed_filepath)
    except shutil.SameFileError:
        print("Source and destination represents the same file.")
    except:
        print("Error occurred while copying file.")
    os.remove(executions_filepath)


    executions_df = executions_df.drop(columns=['FifoPnlRealized', 'MtmPnl'])
    executions_df['ExecutionID'] = [uuid.uuid4() for x in range(executions_df.shape[0])]
    executions_df.set_index('ExecutionID', inplace=True)
    executions_df = executions_df.sort_values('DateTime')
    executions_df.to_sql(name='test_executions', if_exists='append', con=db)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['user@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}


dag = DAG('add_executions_to_database_file_trigger',
          default_args=default_args,
          description='Insert new trade executions to database',
          catchup=False,
          start_date=datetime(2022, 6, 23),
          schedule_interval='@once'
          )

add_executions_task = PythonOperator(task_id='add_executions_task',
                                   python_callable=add_executions,
                                   provide_context=True,
                                   dag=dag)

add_executions_task
