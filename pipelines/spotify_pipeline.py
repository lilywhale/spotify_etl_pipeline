import pandas as pd

from etls.spotify_artists_etl import get_artist_genre, process_artist_data
from etls.spotify_tracks_etl import get_playlist_tracks
from etls.aws_etl import get_secret
from etls.spotify_etl import get_access_token
from utils.constants import REGION_NAME, SECRET_NAME, OUTPUT_PATH, MOST_LISTENED_TRACKS_PLAYLIST_ID


def fetch_spotify_secret(**kwargs) -> None:
    """
    Fetches the spotify secret from aws then call the spottify API to get the access token.
    """
    # Get access token
    secret = get_secret(region_name=REGION_NAME, secret_name=SECRET_NAME)
    client_id = secret['CLIENT_ID']
    client_secret = secret['CLIENT_SECRET']

    access_token = get_access_token(client_id, client_secret)
    print("Successfully fetched access token.")
    print(access_token)
    # Save access_token to XComs
    kwargs['ti'].xcom_push(key='access_token', value=access_token)


def fetch_playlist_tracks(file_postfix: str, **kwargs) -> None:
    """
    Fetches playlist tracks and saves them to a CSV file.
    """
    # Load access_token from XComs
    access_token = kwargs['ti'].xcom_pull(key='access_token')
    print(access_token)

    df_tracks = get_playlist_tracks(MOST_LISTENED_TRACKS_PLAYLIST_ID, access_token)
    if df_tracks is not None:
        file_path = f'{OUTPUT_PATH}//tracks_{file_postfix}.csv'
        df_tracks.to_csv(file_path, index=False)
        print("Successfully fetched playlist tracks and saved csv.")
        # Push file path to XComs
        kwargs['ti'].xcom_push(key='return_value', value=file_path)
        return file_path
    else:
        print("Failed to fetch playlist tracks. Exiting pipeline.")
        return


def fetch_artist_genres(file_postfix: str, **kwargs) -> None:
    """
    Fetches artist genres and saves them to a CSV file.
    """
    # Load access_token from XComs
    access_token = kwargs['ti'].xcom_pull(key='access_token')

    # Load df_tracks from the CSV file
    file_path = f'{OUTPUT_PATH}/tracks_{file_postfix}.csv'
    df_tracks = pd.read_csv(file_path)

    if df_tracks is None:
        raise ValueError("No tracks data available. Run fetch_playlist_tracks first.")

    # Fetch artist genres
    df_artist_genres = get_artist_genre(access_token, df_tracks)
    if df_artist_genres is not None:
        df_artist_genres_clean = process_artist_data(df_artist_genres)

        file_path = f'{OUTPUT_PATH}/artist_genres_{file_postfix}.csv'
        df_artist_genres_clean.to_csv(file_path, index=False)
        print("Successfully fetched artist genres and saved csv.")
        # Push file path to XComs
        kwargs['ti'].xcom_push(key='return_value', value=file_path)
        return file_path
    else:
        print("Failed to fetch artist genres. Continuing pipeline.")
