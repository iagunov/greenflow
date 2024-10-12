Greenflow project

This project is an Airflow-based ETL (Extract, Transform, Load) pipeline that automates the process of fetching data from an external API, uploading it to an S3 bucket, and performing a data transformation using Greenplum.

Key components:

	1.	Data Fetching and S3 Upload:
		The fetch_and_upload_to_s3() function retrieves JSON data from a specified API and uploads it to an S3 bucket. The API URL, bucket name, and object name are dynamically configurable using Airflow Variables, ensuring flexibility across environments.
	2.	Greenplum Data Transformation:
		The run_greenplum_transformation() function connects to a Greenplum database and performs a transformation. It reads data from the S3 bucket as an external table, compares it to an existing Greenplum table, and inserts the unmatched records into a result table.
	3.	Airflow DAG:
		The project is structured as an Airflow Directed Acyclic Graph (DAG), which schedules the execution of the tasks on a daily basis. First, the data is fetched and uploaded to S3, and then the transformation process in Greenplum is triggered.

Technologies Used:

	•	Airflow for task orchestration and scheduling.
	•	boto3 for interacting with the S3 service (Yandex Cloud S3 in this case).
	•	psycopg2 for executing SQL queries in the Greenplum database.
	•	requests for fetching data from the API.

This ETL pipeline is modular, reusable, and built for scalability.