import requests
import pandas as pd
import time
import ast
import re


# Function to get audio features for each track in a list of track IDs
def get_artist_genre(access_token: str, df_tracks: pd.DataFrame) -> pd.DataFrame:
    artist_genre = []

    # Get unique artist IDs from df_tracks
    unique_artist_ids = df_tracks['ARTIST_ID'].unique()

    for artist_id in unique_artist_ids:
        url = f"https://api.spotify.com/v1/artists/{artist_id}"
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        # Retry mechanism for handling rate limiting
        retries = 5
        for attempt in range(retries):
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                artist_genre.append(data)  # Append each artist's data to the list
                break  # Exit the retry loop if successful

            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                print(f"Rate limit reached. Retrying in {retry_after} seconds...")
                time.sleep(retry_after)

            else:
                print(f"Failed to retrieve data for artist {artist_id}: {response.status_code}")
                break  # Exit the loop for any other status code

    # Convert list of artist genres to a DataFrame for easier CSV export
    df = pd.DataFrame(artist_genre)

    df.to_csv('./data/artist_genres.csv', index=False, encoding='utf-8')

    return df


def extract_image_url(images: str) -> str:
    # Parse string representation of the list to actual list
    images_list = ast.literal_eval(images)
    # Filter for highest resolution (640x640) if available
    high_res_image = next((img['url'] for img in images_list if img['height'] == 640), images_list[0]['url'])
    return high_res_image


# Extract followers total
def extract_followers(followers):
    followers_dict = ast.literal_eval(followers)
    return followers_dict['total']


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


def process_artist_data(df: pd.DataFrame) -> pd.DataFrame:

    # Drop unnecessary columns
    df = df.drop(columns=['external_urls', 'uri'])
    df = df.drop_duplicates(subset='id', keep='first')

    df['followers'] = df['followers'].apply(extract_followers)
    df['image_url'] = df['images'].apply(extract_image_url)

    # Drop the original images column
    df = df.drop(columns=['images', 'type'])

    # Convert genres from string to list
    df['genres'] = df['genres'].apply(ast.literal_eval)
    df = df[df['genres'].apply(lambda x: isinstance(x, list) and len(x) > 0)]

    # Explode genres to create one row per artist-genre combination
    df = df.explode('genres').reset_index(drop=True)

    # Apply the cleaning function to 'track_name' and 'album_name'
    df['name'] = df['name'].apply(clean_name)

    df = df.rename(columns={
        'id': 'ARTIST_ID',
        'name': 'ARTIST_NAME',
        'genres': 'GENRES',
        'followers': 'FOLLOWERS',
        'popularity': 'ARTIST_POPULARITY',
        'href': 'HREF',
        'image_url': 'IMAGE_URL'
    })

    # Reorder columns
    column_order = [
        'ARTIST_ID',
        'ARTIST_NAME',
        'GENRES',
        'FOLLOWERS',
        'ARTIST_POPULARITY',
        'HREF',
        'IMAGE_URL'
    ]
    df = df[column_order]

    return df
