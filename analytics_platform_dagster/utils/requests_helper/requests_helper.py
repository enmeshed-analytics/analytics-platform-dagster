import requests
import json

def return_json(url_link: str) -> json:
    try:
        response = requests.get(url_link)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as error:
        print(f"An error occurred: {error}")
        raise


def stream_json(url: str, set_chunk: int) -> json:
    try:
        response = requests.get(url, stream=True)
        buffer = ''
        for chunk in response.iter_content(set_chunk):
            buffer += chunk.decode('utf-8')
        return json.loads(buffer) 
    except requests.RequestException as error:
        print(f"An error occurred: {error}")
        raise
