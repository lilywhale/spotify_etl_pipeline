import requests
import pandas as pd
from typing import Optional
from datetime import datetime
import re


# Function to clean text (removes special characters, emojis, etc.)
def clean_name(name: str) -> str:
    if pd.isnull(name):  # Handle NaN values
        return ""
    # Remove emojis and special characters including commas
    name = re.sub(r'[^\w\s]', '', name, flags=re.UNICODE)
    # Strip leading and trailing spaces
    name = name.strip()
    # Encode and decode to ensure UTF-8 compatibility
    name = name.encode('utf-8', 'ignore').decode('utf-8')
    return name


def fix_date(date_str):
    if len(date_str) == 4:  # Only year is provided
        return f"{date_str}-01-01"  # Default to January 1st of the year
    else:
        return date_str


# Function to fetch playlist tracks
def get_playlist_tracks(playlist_id: str, access_token: str) -> Optional[pd.DataFrame]:
    """
    Fetches tracks from a Spotify playlist and returns a list of processed track data.
    """
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve data: {response.status_code}")
        return None

    data = response.json()
    processed_data = []

    for item in data['items']:
        track = item['track']

        # Clean track and album names
        track_name = clean_name(track['name'])
        album_name = clean_name(track['album']['name'])

        # Fetch the first artist's name and ID
        first_artist = track['artists'][0] if track['artists'] else None
        artist_name = clean_name(first_artist['name']) if first_artist else None
        artist_id = first_artist['id'] if first_artist else None

        # Prepare track data
        track_data = {
            'TRACK_NAME': track_name,
            'TRACK_ID': track['id'],
            'ARTIST_NAME': artist_name,
            'ARTIST_ID': artist_id,
            'ALBUM_NAME': album_name,
            'RELEASE_DATE': track['album']['release_date'],
            'TOTAL_TRACKS': track['album']['total_tracks'],
            'DURATION_SEC': track['duration_ms'] / 1000,  # Convert ms to seconds
            'EXPLICIT': track['explicit'],
            'POPULARITY': track['popularity'],
            'ISRC': track['external_ids'].get('isrc'),
            'PREVIEW_URL': track.get('preview_url'),
            'TRACK_SPOTIFY_URL': track['external_urls'].get('spotify'),
            'ALBUM_SPOTIFY_URL': track['album']['external_urls'].get('spotify')
        }

        processed_data.append(track_data)

        df_tracks = pd.DataFrame(processed_data)

        # Adding the rank column to keep track of the daily rank
        df_tracks['STREAM_RANK'] = df_tracks.index + 1
        manual_date = datetime.now().strftime("%Y%m%d")
        df_tracks['DATE_RANK'] = manual_date
        df_tracks['RELEASE_DATE'] = df_tracks['RELEASE_DATE'].apply(fix_date)

        df_tracks.to_csv('./data/top_tracks.csv', index=False, encoding='utf-8')

    return df_tracks
