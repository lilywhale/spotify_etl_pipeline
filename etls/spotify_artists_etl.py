import requests
import pandas as pd
import time
import re


# Function to get audio features for each track in a list of track IDs
def get_artist_genre(access_token: str, df_tracks: pd.DataFrame) -> pd.DataFrame:
    artist_genre = []

    # Get unique artist IDs from df_tracks
    unique_artist_ids = df_tracks['ARTIST_ID'].unique()
    print(len(unique_artist_ids))

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


def extract_image_url(images):
    """
    Extracts the highest resolution image URL from the images list.
    """
    if isinstance(images, list) and len(images) > 0:
        # Filter for highest resolution (640x640) if available
        high_res_image = next((img['url'] for img in images if img.get('height') == 640), images[0]['url'])
        return high_res_image
    return None


# Extract followers total
def extract_followers(followers):
    """
    Extracts the total number of followers from the followers dictionary.
    """
    if isinstance(followers, dict):
        return followers.get('total', 0)
    return 0


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
    """
    Processes the artist data DataFrame.
    """
    # Drop unnecessary columns
    df = df.drop(columns=['external_urls', 'uri'])
    df = df.drop_duplicates(subset='id', keep='first')

    # Extract followers and image URL
    df['followers'] = df['followers'].apply(extract_followers)
    df['image_url'] = df['images'].apply(extract_image_url)

    # Drop the original images column
    df = df.drop(columns=['images', 'type'])

    # Ensure genres is a list
    df['genres'] = df['genres'].apply(lambda x: x if isinstance(x, list) else [])

    # Filter out artists with no genres
    df = df[df['genres'].apply(lambda x: len(x) > 0)]

    # Explode genres to create one row per artist-genre combination
    df = df.explode('genres').reset_index(drop=True)

    # Clean artist names
    df['name'] = df['name'].apply(clean_name)

    # Rename columns
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
