import requests
import json

from typing import Union


def return_json(url_link: str) -> Union[dict, list]:
    """
    Simple json get request

    Args:
        Url (e.g. api endpoint): str

    Returns:
        json
    """
    try:
        response = requests.get(url_link)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as error:
        print(f"An error occurred: {error}")
        raise


def stream_json(url: str, set_chunk: int):
    """
    Streams a larger json file into memory

    Args:
        Url (e.g. api endpoint): str

    Returns:
        json
    """
    try:
        response = requests.get(url, stream=True)
        buffer = ""
        for chunk in response.iter_content(set_chunk):
            buffer += chunk.decode("utf-8")
        return json.loads(buffer)
    except requests.RequestException as error:
        print(f"An error occurred: {error}")
        raise


def fetch_redirect_url(url) -> str:
    """
    Call the redirect url and then fetch the actual download url.
    """

    if url is None:
        raise ValueError("url is empty")

    try:
        response = requests.get(url)
        response.raise_for_status()
        redirect_url = response.url
        print(f"The Redirect URL is: {redirect_url}")
    except (requests.exceptions.RequestException, ValueError, Exception) as e:
        print(f"An error retrieving the redirect URL: {e}")
        raise
    return redirect_url
