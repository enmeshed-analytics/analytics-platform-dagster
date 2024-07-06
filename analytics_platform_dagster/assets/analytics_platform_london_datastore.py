import requests
import pandas as pd

from dagster import asset

@asset
def london_datastore() -> pd.DataFrame:
    """
    Fetch data from London Datastore API

    Need to process each url where the resource format is excel and dummp to s3.
    Need to add to jobs and jobs definition
    """

    url = "https://data.london.gov.uk/api/action/package_search"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        results = data['result']['result']

        # Use list comprehension for processing
        extracted_data = [
            {
                'maintainer': result.get('maintainer'),
                'title': result.get('title'),
                'id': result.get('id'),
                'resource_url': resource.get('url'),
                'resource_format': resource.get('format'),
                'resource_created': resource.get('created'),
                'resource_last_modified': resource.get('last_modified')
            }
            for result in results
            for resource in result.get('resources', [])
        ]

        # Create DataFrame once at the end
        df = pd.DataFrame(extracted_data)

        return df
    else:
        raise Exception(f"Failed to fetch data: Status code {response.status_code}")
