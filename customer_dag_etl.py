from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy as db
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ========= EDIT THESE TWO LINES =========
CSV_PATH = "/Users/fidan/Documents/Customer_ETL/Bakery_Customer_Data.csv"
CONNECTION_URI = "postgresql+psycopg2://postgres:UPmeaGWVSMXkvkVkElzYZOhYNgPqqGDr@kodama.proxy.rlwy.net:28228/railway"
# ========================================

def run_etl():
    # --- Extract ---
    customer_data = pd.read_csv(CSV_PATH)

    # --- Transform ---
    customer_data = customer_data.drop_duplicates(subset=['customerID', 'email'])
    customer_data['phone'] = customer_data['phone'].fillna('Unknown')

    customer_data['address']      = customer_data['delivery_address'].str.split(pat=',').str[0]
    customer_data['country_code'] = customer_data['delivery_address'].str.split(pat=',').str[1]
    customer_data['zipcode']      = customer_data['delivery_address'].str.split(pat=',').str[2]

    customer_data['last_delivery_date'] = customer_data['last_delivery'].str.split(pat=',').str[0]
    customer_data['last_delivery_time'] = customer_data['last_delivery'].str.split(pat=',').str[1]

    customer_data = customer_data.drop(['last_delivery', 'delivery_address'], axis=1)
    customer_data = customer_data.rename(columns={'birthday': 'birth_date', 'status': 'loyalty_status'})

    # --- Load ---
    engine = db.create_engine(CONNECTION_URI)
    customer_data.to_sql("customers", engine, if_exists='replace', index=False)
    print(f"ETL successful! Loaded {len(customer_data)} rows.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='customer_full_load',
    default_args=default_args,
    description='Customer Table ETL DAG',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:
    PythonOperator(
        task_id='run_customer_etl',
        python_callable=run_etl,
    )
