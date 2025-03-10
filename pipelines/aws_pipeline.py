from etls.aws_etl import upload_to_s3
import pandas as pd
from utils.constants import BUCKET_NAME, REGION_NAME


def upload_s3_pipeline(**kwargs):
    """
    Uploads all CSV files to S3.
    """
    ti = kwargs['ti']

    # Load file paths from XComs
    tracks_file_path = ti.xcom_pull(task_ids='fetch_playlist_tracks', key='return_value')
    artist_genre_file_path = ti.xcom_pull(task_ids='fetch_artist_genres', key='return_value')

    # Upload tracks data
    if tracks_file_path:
        df_tracks = pd.read_csv(tracks_file_path)
        upload_to_s3(df_tracks, BUCKET_NAME, f"tracks/{tracks_file_path.split('/')[-1]}", REGION_NAME)

    # Upload artist genres data
    if artist_genre_file_path:
        df_artist_genres = pd.read_csv(artist_genre_file_path)
        upload_to_s3(df_artist_genres, BUCKET_NAME, f"artist_genres/{artist_genre_file_path.split('/')[-1]}", REGION_NAME)
