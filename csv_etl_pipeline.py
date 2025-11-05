from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Your Neon direct connection (no pooler)
DB_URL = "postgresql://neondb_owner:npg_LcgUd2By5kIK@ep-wispy-bonus-a14cvbmp.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

def extract_csv(**context):
    """Extract: Read CSV file"""
    df = pd.read_csv('/opt/airflow/dags/sales_data.csv') # Airflow container path
    print(f"Extracted {len(df)} rows from CSV")
    context['task_instance'].xcom_push(key='raw_df', value=df.to_json())
    return len(df)

def transform_data(**context):
    """Transform: Clean and filter North region"""
    df = pd.read_json(context['task_instance'].xcom_pull(task_ids='extract_csv', key='raw_df'))
    # Add tax and grand total
    df['Tax'] = df['Total'] * 0.10
    df['GrandTotal'] = df['Total'] + df['Tax']
    # Filter North region only
    df_north = df[df['Region'] == 'North']
    print(f"Transformed: {len(df_north)} North rows (original: {len(df)})")
    context['task_instance'].xcom_push(key='transformed_df', value=df_north.to_json())
    return len(df_north)

def load_to_postgres(**context):
    """Load: Save to Neon PostgreSQL (bypass proxy for SSL)"""
    df = pd.read_json(context['task_instance'].xcom_pull(task_ids='transform_data', key='transformed_df'))
    
    # Bypass proxy for Neon SSL connection
    original_proxy = os.environ.get('https_proxy')
    if original_proxy:
        del os.environ['https_proxy']
        del os.environ['http_proxy']
    
    try:
        from sqlalchemy import create_engine
        engine = create_engine(
            DB_URL,
            connect_args={"connect_timeout": 60},
            pool_pre_ping=True
        )
        df.to_sql('sales_table', engine, if_exists='append', index=False)
        print(f"Loaded {len(df)} rows to Neon PostgreSQL!")
    finally:
        # Restore proxy
        if original_proxy:
            os.environ['https_proxy'] = original_proxy
            os.environ['http_proxy'] = original_proxy

# Full DAG
with DAG(
    dag_id='csv_etl_pipeline',
    start_date=datetime(2025, 11, 4),
    schedule='@daily',
    catchup=False,
    tags=['etl', 'germany', 'portfolio']
) as dag:

    extract = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_csv
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    extract >> transform >> load