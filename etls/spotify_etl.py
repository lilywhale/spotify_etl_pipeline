import base64
import requests


def get_access_token(client_id: str, client_secret: str) -> str:
    auth_url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Authorization': 'Basic ' + base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode(),
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = {'grant_type': 'client_credentials'}

    response = requests.post(auth_url, headers=headers, data=data)
    response_json = response.json()
    print(response)
    response.raise_for_status()
    return response_json['access_token']
