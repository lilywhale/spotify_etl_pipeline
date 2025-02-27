from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
import sys

from pipelines.spotify_pipeline import fetch_spotify_secret, fetch_playlist_tracks, fetch_artist_genres, fetch_audio_features
from pipelines.aws_pipeline import upload_s3_pipeline

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner': 'Camille JMML',
    'start_date': datetime(2025, 2, 21)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='spotify_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['spotify', 'etl', 'pipeline']
)

# Task 1: Fetch Spotify secret
fetch_secret = PythonOperator(
    task_id='fetch_spotify_secret',
    python_callable=fetch_spotify_secret,
    provide_context=True,
    dag=dag
)

# Task 2: Fetch playlist tracks
fetch_tracks = PythonOperator(
    task_id='fetch_playlist_tracks',
    python_callable=fetch_playlist_tracks,
    op_kwargs={'file_postfix': file_postfix},
    provide_context=True,
    dag=dag
)

# Task 3: Fetch artist genres
fetch_artists = PythonOperator(
    task_id='fetch_artist_genres',
    python_callable=fetch_artist_genres,
    op_kwargs={'file_postfix': file_postfix},
    provide_context=True,
    dag=dag
)

# Task 4: Fetch track audio features
fetch_audio = PythonOperator(
    task_id='fetch_audio_features',
    python_callable=fetch_audio_features,
    op_kwargs={'file_postfix': file_postfix},
    provide_context=True,
    dag=dag
)

# Task 5: Upload all data to S3
upload_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_s3_pipeline,
    provide_context=True,
    dag=dag
)

# Define task dependencies
fetch_secret >> fetch_tracks >> [fetch_artists, fetch_audio] >> upload_s3
