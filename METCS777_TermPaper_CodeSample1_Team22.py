from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'Keerthi',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 28),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_amazon_reviews_dag',
    default_args=default_args,
    description='ETL Workflow for Aggregating Amazon Product Reviews Data',
    schedule=timedelta(days=1),
    catchup=False,
)

DATA_DIR = '/opt/airflow/data'  # Mounted data folder inside Docker

def extract_data(**kwargs):
    file_path = f'{DATA_DIR}/amazon.csv'
    output_path = f'{DATA_DIR}/extracted_data.csv'
    df = pd.read_csv(file_path)
    df.to_csv(output_path, index=False)
    kwargs['ti'].xcom_push(key='extracted_data_path', value=output_path)

def transform_data(**kwargs):
    ti = kwargs['ti']
    input_path = ti.xcom_pull(task_ids='extract', key='extracted_data_path')
    df = pd.read_csv(input_path)
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    avg_rating_by_category = df.groupby('category')['rating'].mean().reset_index()
    avg_rating_by_category.columns = ['Category', 'AverageRating']
    output_path = f'{DATA_DIR}/transformed_data.csv'
    avg_rating_by_category.to_csv(output_path, index=False)
    ti.xcom_push(key='transformed_data_path', value=output_path)

def load_data(**kwargs):
    ti = kwargs['ti']
    input_path = ti.xcom_pull(task_ids='transform', key='transformed_data_path')
    transformed_data = pd.read_csv(input_path)
    output_path = f'{DATA_DIR}/transformed_amazon_reviews.csv'
    transformed_data.to_csv(output_path, index=False)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

extract >> transform >> load