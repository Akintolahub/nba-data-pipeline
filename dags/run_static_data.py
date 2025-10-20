from datetime import datetime, timedelta
import pandas as pd
from google.oauth2 import service_account
from airflow import DAG
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.standard.operators.python import PythonOperator
from nba_api.stats.static import players, teams

def get_nba_players():
    raw_playerdata = players.get_players()
    player_df = pd.DataFrame(raw_playerdata)
    player_df.to_csv('/opt/airflow/datasets/nba_players.csv', index=False)
    print("✅ All NBA player data retrieved and stored as csv")

    gcs_bucket_name = 'raw_nba_dataset'
    gcs_file_path = 'nba_players.csv'
    
    try:
        credentials = service_account.Credentials.from_service_account_file('/opt/airflow/gcp-credentials/nfl1-447014-d54dfe0c999f.json')
        client = storage.Client(credentials=credentials, project=credentials.project_id)
        bucket = client.bucket(gcs_bucket_name)
        
        blob = bucket.blob(gcs_file_path)
        
        player_data_csv = player_df.to_csv(index=False)
        blob.upload_from_string(player_data_csv, content_type='csv')

        print(f"✅ Successfully uploaded player data to gs://{gcs_bucket_name}/{gcs_file_path}")

    except Exception as e:
        print(f"❌ Failed to upload to GCS: {e}") 
    return len(player_df)

def get_nba_teams():

    raw_teamdata = teams.get_teams()
    df_teamdata = pd.DataFrame(raw_teamdata)
    df_teamdata.to_csv('/opt/airflow/datasets/nba_teams.csv', index=False)
    print("✅ All NBA player teams retrieved and stored as csv")

    gcs_bucket_name = 'raw_nba_dataset'
    gcs_file_path = 'nba_teams.csv'
    
    try:
        credentials = service_account.Credentials.from_service_account_file('/opt/airflow/gcp-credentials/nfl1-447014-d54dfe0c999f.json')
        client = storage.Client(credentials=credentials, project=credentials.project_id)
        bucket = client.bucket(gcs_bucket_name)
        
        blob = bucket.blob(gcs_file_path)
        
        team_data_csv = df_teamdata.to_csv(index=False)
        blob.upload_from_string(team_data_csv, content_type='csv')

        print(f"✅ Successfully uploaded team data to gs://{gcs_bucket_name}/{gcs_file_path}")

    except Exception as e:
        print(f"❌ Failed to upload to GCS: {e}") 
    return len(df_teamdata)

default_args = {
    'owner' : 'Tofunmi',
    'depends_on_past': False,
    'start_date': datetime(2025,10,8),
    'email': ['akintolaoluwatofunmi@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}
dag = DAG(
    'local_get_static_nba_data_once_a_year',
    default_args=default_args,
    description='Extracts all static NBA data (teams, players) from the NBA API and loads them into gcp staging table',
    schedule=None,
    catchup=False,
    tags=['local','static','nba']
)
extract_player_data = PythonOperator(
    task_id='extract_nba_player_data',
    python_callable=get_nba_players,
    dag=dag
)
extract_team_data = PythonOperator(
    task_id='extract_nba_team_data',
    python_callable=get_nba_teams,
    dag=dag
)
load_static_players_to_landing = BigQueryInsertJobOperator(
    task_id='load_static_players_to_landing_table',
    gcp_conn_id='google_cloud_default',
    configuration={
        "load": {
            "sourceUris": [
                "gs://raw_nba_dataset/nba_players.csv" 
            ],
            "destinationTable": {
                "projectId": "nfl1-447014", 
                "datasetId": "nba_landing",
                "tableId": "nba_players"
            },
            "sourceFormat": "CSV",
            "autodetect": True,
            "writeDisposition": "WRITE_TRUNCATE", 
            "skipLeadingRows": 1,
            "allowQuotedNewlines": True,
        }
    },
    dag=dag
)
load_static_teams_to_landing = BigQueryInsertJobOperator(
    task_id='load_static_teams_to_landing_table',
    gcp_conn_id='google_cloud_default',
    configuration={
        "load": {
            "sourceUris": [
                "gs://raw_nba_dataset/nba_teams.csv" 
            ],
            "destinationTable": {
                "projectId": "nfl1-447014", 
                "datasetId": "nba_landing",
                "tableId": "nba_teams"
            },
            "sourceFormat": "CSV",
            "autodetect": True,
            "writeDisposition": "WRITE_TRUNCATE", 
            "skipLeadingRows": 1,
            "allowQuotedNewlines": True,
        }
    },
    dag=dag
)
load_static_players_to_staging = BigQueryInsertJobOperator(
    task_id='load_static_players_to_staging_table',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": "{% include 'sql/staging_nba_players.sql' %}",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "nfl1-447014",
                "datasetId": "nba_staging",
                "tableId": "nba_players"
            },
            "writeDisposition": "WRITE_TRUNCATE", 
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    dag=dag
)
load_static_teams_to_staging = BigQueryInsertJobOperator(
    task_id='load_static_teams_to_staging_table',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": "{% include 'sql/staging_nba_teams.sql' %}",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "nfl1-447014",
                "datasetId": "nba_staging",
                "tableId": "nba_teams"
            },
            "writeDisposition": "WRITE_TRUNCATE", 
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    dag=dag
)
load_static_players_to_data_mart = BigQueryInsertJobOperator(
    task_id='load_static_players_to_data_mart',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": "{% include 'sql/dim_nba_players.sql' %}",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "nfl1-447014",
                "datasetId": "nba_marts",
                "tableId": "dim_nba_players"
            },
            "writeDisposition": "WRITE_TRUNCATE", 
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    dag=dag
)
load_static_teams_to_data_mart = BigQueryInsertJobOperator(
    task_id='load_static_teams_to_data_mart',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": "{% include 'sql/dim_nba_teams.sql' %}",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "nfl1-447014",
                "datasetId": "nba_marts",
                "tableId": "dim_nba_teams"
            },
            "writeDisposition": "WRITE_TRUNCATE", 
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    dag=dag
)
    
extract_player_data >> load_static_players_to_landing >> load_static_players_to_staging >> load_static_players_to_data_mart
extract_team_data >> load_static_teams_to_landing >> load_static_teams_to_staging >> load_static_teams_to_data_mart