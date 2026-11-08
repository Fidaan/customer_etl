# Customer ETL Pipeline with Apache Airflow
An ETL (Extract, Transform, Load) pipeline that processes bakery customer data. It extracts raw customer records from a CSV file, cleans and reshapes them with pandas, and loads the result into a PostgreSQL database. The whole process is orchestrated and scheduled with Apache Airflow.

Pipeline
extract – read the raw Bakery_Customer_Data.csv and stage it
transform – clean and reshape the data (see below)
load – write the cleaned table into PostgreSQL (customers table)

Transformations applied
* Remove duplicate customers (same customerID + email)
* Fill missing phone numbers with "Unknown"
* Split delivery_address → address, country_code, zipcode
* Split last_delivery → last_delivery_date, last_delivery_time
* Rename birthday → birth_date and status → loyalty_status

Tech stack
Python 3
pandas – data manipulation
SQLAlchemy + psycopg2 – PostgreSQL connectivity
PostgreSQL (hosted on Railway)
Apache Airflow – orchestration & scheduling

Setup
1)Create and activate a virtual environment:
python3 -m venv venv
source venv/bin/activate          # macOS / Linux
2)Install dependencies:
pip install -r requirements.txt
3)Configure your database connection and data path as environment variables (the DAG reads these):
export CUSTOMER_DB_URI="postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DBNAME"
export CUSTOMER_CSV_PATH="/full/path/to/Bakery_Customer_Data.csv"

Running with Airflow
export AIRFLOW_HOME=~/airflow # place customer_dag_etl.py in ~/airflow/dags/
airflow standalone

Then open the Airflow UI at http://localhost:8080, find the customer_full_load DAG, enable it, and trigger a run. Watch the three tasks (extract → transform → load) complete in the Graph view.

### Airflow DAG
The pipeline running successfully in Apache Airflow:
<img width="2000" height="1235" alt="image" src="https://github.com/user-attachments/assets/8bc2d581-52f0-4361-9988-a6018c281f5d" />

