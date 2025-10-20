from datetime import datetime, timedelta
from airflow import DAG 
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pandas as pd, os
from google.oauth2 import service_account
from airflow import DAG 
from google.cloud import storage
from airflow.providers.standard.operators.python import PythonOperator 
from nba_api.stats.endpoints import leaguegamefinder,infographicfanduelplayer 

def get_game_ids():
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2025-26')
    fields = ['SEASON_ID','TEAM_ID','GAME_ID','GAME_DATE','MATCHUP']
    df_games = gamefinder.get_data_frames()[0][fields]

    unique_game_ids = df_games['GAME_ID'].unique()
    print(f"✅ Found {len(unique_game_ids)} NBA game IDs to process.")
    
    gcs_bucket_name = 'raw_nba_dataset'
    gcs_file_path = 'nba_game_ids.csv'
    
    try:
        credentials = service_account.Credentials.from_service_account_file('/opt/airflow/gcp-credentials/nfl1-447014-d54dfe0c999f.json')
        client = storage.Client(credentials=credentials, project=credentials.project_id)
        bucket = client.bucket(gcs_bucket_name)
        
        blob = bucket.blob(gcs_file_path)
        
        csv_data = df_games.to_csv(index=False)
        blob.upload_from_string(csv_data, content_type='csv')
        
        print(f"✅ Successfully uploaded {len(unique_game_ids)} game IDs to gs://{gcs_bucket_name}/{gcs_file_path}")
        
    except Exception as e:
        print(f"❌ Failed to upload to GCS: {e}") 
    return unique_game_ids.tolist()

def get_fantasy_stats(**context):
    ti = context['ti']
    game_ids = ti.xcom_pull(task_ids='extract_nba_game_ids')
    
    output_dir = '/opt/airflow/datasets/nba_player_fantasy_data'
    os.makedirs(output_dir, exist_ok=True)

    existing_gcs_files = get_existing_gcs_files()
    print(f"Found {len(existing_gcs_files)} existing files in GCS")
    
    new_files_uploaded = 0
    
    for game_id in game_ids:
        file_name = f'nba_player_fantasydata_{game_id}.csv'
        gcs_path = f'nba_player_fantasy_data/{file_name}'
        
        if gcs_path in existing_gcs_files:
            print(f"File {file_name} already exists in GCS. Skipping.")
            continue
            
        local_file_path = os.path.join(output_dir, file_name)

        if os.path.exists(local_file_path):
            print(f"Uploading existing local file to GCS: {file_name}")
            upload_single_file_to_gcs(local_file_path, file_name)
            new_files_uploaded += 1
            continue

        try:
            print(f"Fetching NEW data for game ID: {game_id}")
            
            fantasy_data = infographicfanduelplayer.InfographicFanDuelPlayer(game_id=game_id).get_data_frames()[0]
            fantasy_data['GAME_ID'] = game_id
            fantasy_data.to_csv(local_file_path, index=False)
            print(f"Successfully saved {len(fantasy_data)} rows to {local_file_path}")
            
            upload_single_file_to_gcs(local_file_path, file_name)
            new_files_uploaded += 1
            
        except Exception as e:
            print(f"Error fetching data for game ID {game_id}: {e}")
            continue
    
    print(f"Uploaded {new_files_uploaded} new files to GCS")
    return new_files_uploaded

def get_existing_gcs_files():
    """Get list of all files already in GCS fantasy data folder"""
    try:
        credentials = service_account.Credentials.from_service_account_file('/opt/airflow/gcp-credentials/nfl1-447014-d54dfe0c999f.json')
        client = storage.Client(credentials=credentials, project=credentials.project_id)
        bucket = client.bucket('raw_nba_dataset')
        
        blobs = bucket.list_blobs(prefix='nba_player_fantasy_data/')
        
        existing_files = []
        for blob in blobs:
            if blob.name.endswith('.csv'):
                existing_files.append(blob.name)
        
        return existing_files
        
    except Exception as e:
        print(f"Failed to list GCS files: {e}")
        return []

def upload_single_file_to_gcs(local_file_path, file_name):
    """Upload a single file to GCS"""
    try:
        credentials = service_account.Credentials.from_service_account_file('/opt/airflow/gcp-credentials/nfl1-447014-d54dfe0c999f.json')
        client = storage.Client(credentials=credentials, project=credentials.project_id)
        bucket = client.bucket('raw_nba_dataset')
        
        gcs_path = f'nba_player_fantasy_data/{file_name}'
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_file_path)
        
        print(f"✅ Uploaded {file_name} to gs://raw_nba_dataset/{gcs_path}")

    except Exception as e:
        print(f"Failed to upload {file_name} to GCS: {e}")

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
    'local_get_daily_nba_stats_dag',
    default_args=default_args,
    description='Extracts all active NBA players stats (normal and fantasy) from the NBA API and loads them into gcp landing table',
    schedule='0 7 * * *',
    tags=['nba','daily','stats','gcp','bigquery','local'],
)
extract_game_ids = PythonOperator(
    task_id='extract_nba_game_ids',
    python_callable=get_game_ids,
    dag=dag
)
extract_fantasy_stats = PythonOperator(
    task_id='extract_nba_fantasy_stats',
    python_callable=get_fantasy_stats,
    dag=dag
)
load_game_ids_to_landing = BigQueryInsertJobOperator(
    task_id='load_game_ids_to_landing_table',
    gcp_conn_id='google_cloud_default',
    configuration={
        "load": {
            "sourceUris": [
                "gs://raw_nba_dataset/nba_game_ids.csv" 
            ],
            "destinationTable": {
                "projectId": "nfl1-447014", 
                "datasetId": "nba_landing",
                "tableId": "nba_game_ids"
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
load_daily_stats_to_landing = BigQueryInsertJobOperator(
    task_id='load_daily_stats_to_landing_table',
    gcp_conn_id='google_cloud_default', 
    configuration={
        "load": {
            "sourceUris": [
                "gs://raw_nba_dataset/nba_player_fantasy_data/*.csv" 
            ],
            "destinationTable": {
                "projectId": "nfl1-447014",
                "datasetId": "nba_landing",
                "tableId": "nba_daily_stats"
            },
            "sourceFormat": "CSV",
            "autodetect": False,
            "writeDisposition": "WRITE_APPEND", 
            "skipLeadingRows": 1,
            "allowQuotedNewlines": True,
        }
    },
    dag=dag
)
load_game_ids_to_staging = BigQueryInsertJobOperator(
    task_id='load_game_ids_to_staging_table',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": "{% include 'sql/staging_game_ids.sql' %}",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "nfl1-447014",
                "datasetId": "nba_staging",
                "tableId": "nba_game_ids"
            },
            "writeDisposition": "WRITE_APPEND", 
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    dag=dag
)
load_daily_stats_to_staging = BigQueryInsertJobOperator(
    task_id='load_daily_stats_to_staging_table',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": "{% include 'sql/staging_daily_stats.sql' %}",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "nfl1-447014",
                "datasetId": "nba_staging",
                "tableId": "nba_daily_stats"
            },
            "writeDisposition": "WRITE_APPEND", 
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    dag=dag
)
load_game_ids_to_data_mart = BigQueryInsertJobOperator(
    task_id='load_game_ids_to_data_mart',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": "{% include 'sql/dim_game_ids.sql' %}",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "nfl1-447014",
                "datasetId": "nba_marts",
                "tableId": "dim_game_ids"
            },
            "writeDisposition": "WRITE_TRUNCATE", 
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    dag=dag
)
load_daily_stats_to_data_mart = BigQueryInsertJobOperator(
    task_id='load_daily_stats_to_data_mart',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": "{% include 'sql/fact_daily_stats.sql' %}",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "nfl1-447014",
                "datasetId": "nba_marts",
                "tableId": "fact_nba_fantasy_stats"
            },
            "writeDisposition": "WRITE_TRUNCATE", 
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    dag=dag
)
create_daily_fantasy_stats_view = BigQueryInsertJobOperator(
    task_id='create_daily_top_performers_view',
    gcp_conn_id='google_cloud_default',
    configuration={
        "query": {
            "query": "{% include 'sql/view_fantasy_stats.sql' %}",
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "nfl1-447014", 
                "datasetId": "nba_marts",
                "tableId": "view_daily_fantasy_stats"
            },
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_IF_NEEDED",
        }
    },
    dag=dag
)

extract_game_ids >> [load_game_ids_to_landing, extract_fantasy_stats]
extract_fantasy_stats >> load_daily_stats_to_landing

load_game_ids_to_landing >> load_game_ids_to_staging >> load_game_ids_to_data_mart
load_daily_stats_to_landing >> load_daily_stats_to_staging >> load_daily_stats_to_data_mart

[load_game_ids_to_data_mart, load_daily_stats_to_data_mart] >> create_daily_fantasy_stats_view