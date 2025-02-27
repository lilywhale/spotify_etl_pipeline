import requests
import pandas as pd
import re
from typing import Optional


# Function to get audio features for each track in a list of track IDs
def get_track_audio_features(access_token: str,  df_tracks: pd.DataFrame) -> Optional[pd.DataFrame]:
    track_features = []

    tracks_ids = df_tracks['TRACK_ID']

    for track_id in tracks_ids:

        url = f"https://api.spotify.com/v1/audio-features/{track_id}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Failed to retrieve audio features for track {track_id}: {response.status_code}")
            return None

        data = response.json()

        track_features.append(data)  # Append each track's data to the list

    # Convert list of track features to a DataFrame for easier CSV export
    df = pd.DataFrame(track_features)
    df.to_csv('./data/track_audio_features.csv', index=False, encoding='utf-8')
    return df


# Function to clean the text
def clean_name(name):
    if pd.isnull(name):  # Handle NaN values
        return ""
    # Remove emojis and special characters including commas
    name = re.sub(r'[^\w\s]', '', name, flags=re.UNICODE)
    # Strip leading and trailing spaces
    name = name.strip()
    # Encode and decode to ensure UTF-8 compatibility
    name = name.encode('utf-8', 'ignore').decode('utf-8')
    return name


# Process Track Data
def process_features_data(df: pd.DataFrame) -> pd.DataFrame:
    # Remove duplicates
    df = df.drop_duplicates(subset='id')
    df = df.dropna(subset=['id', 'track_name'])

    # Step 1: Filter rows where type is 'audio_features'
    df = df[df['type'] == 'audio_features']

    # Step 2: Drop 'uri' and 'analysis_url' columns
    df = df.drop(columns=['uri', 'analysis_url'])

    # Step 3: Convert 'duration_ms' to seconds and rename to 'duration_seconds'
    df['duration_seconds'] = df['duration_ms'] / 1000
    df = df.drop(columns=['duration_ms', 'type'])

    # Step 4: Rename columns for consistency and to fit SQL conventions
    df.columns = [col.lower() for col in df.columns]  # Lowercase for SQL compatibility
    df = df.rename(columns={'id': 'track_id'})  # 'id' might be ambiguous; renaming to 'track_id'

    # Optional Step 5: Ensure data types are consistent (e.g., float, int)
    df['danceability'] = df['danceability'].astype(float)
    df['energy'] = df['energy'].astype(float)
    df['key'] = df['key'].astype(int)
    df['loudness'] = df['loudness'].astype(float)
    df['mode'] = df['mode'].astype(int)
    df['speechiness'] = df['speechiness'].astype(float)
    df['acousticness'] = df['acousticness'].astype(float)
    df['instrumentalness'] = df['instrumentalness'].astype(float)
    df['liveness'] = df['liveness'].astype(float)
    df['valence'] = df['valence'].astype(float)
    df['tempo'] = df['tempo'].astype(float)
    df['time_signature'] = df['time_signature'].astype(int)
    df['duration_seconds'] = df['duration_seconds'].astype(float)

    df['track_name'] = df['track_name'].apply(clean_name)

    # Rename columns to match SQL schema
    df = df.rename(columns={
        'danceability': 'DANCEABILITY',
        'energy': 'ENERGY',
        'key': 'SCAL_KEY',
        'loudness': 'LOUDNESS',
        'mode': 'SCALE_MODE',
        'speechiness': 'SPEECHINESS',
        'acousticness': 'ACOUSTICNESS',
        'instrumentalness': 'INSTRUMENTALNESS',
        'liveness': 'LIVENESS',
        'valence': 'VALENCE',
        'tempo': 'TEMPO',
        'track_id': 'TRACK_ID',
        'track_href': 'TRACK_HREF',
        'time_signature': 'TIME_SIGNATURE',
        'track_name': 'TRACK_NAME',
        'duration_seconds': 'DURATION_SECONDS'
    })

    # Reorder columns to match SQL schema
    column_order = [
        'TRACK_NAME',
        'TRACK_ID',
        'DANCEABILITY',
        'ENERGY',
        'SCAL_KEY',
        'LOUDNESS',
        'SCALE_MODE',
        'SPEECHINESS',
        'ACOUSTICNESS',
        'INSTRUMENTALNESS',
        'LIVENESS',
        'VALENCE',
        'TEMPO',
        'TIME_SIGNATURE',
        'DURATION_SECONDS',
        'TRACK_HREF'
    ]
    df = df[column_order]
    return df
