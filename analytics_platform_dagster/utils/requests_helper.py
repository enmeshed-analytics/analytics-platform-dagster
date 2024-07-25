import requests

def return_api_data(url_link: str):
    try:
        response = requests.get(url_link)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as error:
        print(f"An error occurred: {error}")
        raise