from dotenv import load_dotenv
import os


dotenv_path = os.path.join(os.path.dirname(__file__), '../config/config.env')
load_dotenv(dotenv_path=dotenv_path)

# Access variables from config.env
BUCKET_NAME = os.getenv('aws_bucket_name')
REGION_NAME = os.getenv('aws_region')
SECRET_NAME = os.getenv('secret_name')
AWS_ACCESS_KEY = os.getenv('aws_access_key')
AWS_ACCESS_KEY_ID = os.getenv('aws_access_key_id')

SPOTIFY_SECRET = os.getenv('spotify_secret_key')
SPOTIFY_CLIENT_ID = os.getenv('spotify_client_id')


MOST_LISTENED_TRACKS_PLAYLIST_ID = '37i9dQZEVXbMDoHDwVN2tF'

OUTPUT_PATH = os.getenv('output_path')
