import requests
import pandas as pd

from ...utils.variables_helper.url_links import asset_urls
from pprint import pprint


def london_datastore_bronze() -> pd.DataFrame:
    """
    London Datastore Metadata bronze bucket
    """
    url = asset_urls.get("london_data_store")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    pprint(data)
    df = pd.DataFrame(data)
    df = df.astype(str)
    return df
