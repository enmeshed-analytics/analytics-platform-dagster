import asyncio
import aiohttp
import pandas as pd
import io
import os

# This works perfect locally and saves local parquet files...
# See analytics_platform_london_datastore.py for the Dagster asset - needs testing first

async def london_datastore():
    """
    Fetch data from London Datastore API, process Excel files, and save as parquet locally.
    """
    try:
        num_rows = 20
        url = f"https://data.london.gov.uk/api/action/package_search?rows={num_rows}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch data: Status code {response.status}")
                data = await response.json()
            results = data['result']['result']
            output_dir = 'london_datastore_output'
            os.makedirs(output_dir, exist_ok=True)
            tasks = []
            for result in results:
                for resource in result.get('resources', []):
                    if resource.get('format', '').lower() in ['spreadsheet', 'csv']:  # Added 'csv' to include CSV files
                        task = asyncio.create_task(process_file(session, result, resource, output_dir))
                        tasks.append(task)
            await asyncio.gather(*tasks)
        print(f"Finished processing all files. Output saved in {output_dir}")
    except Exception as e:
        print(f"An error occurred in london_datastore: {str(e)}")
        raise

async def process_file(session, result, resource, output_dir):
    """
    Process individual files and save as parquet locally.
    """
    try:
        # Construct the download URL
        dataset_name = result.get('name')
        resource_id = resource.get('id')
        file_name = resource.get('name')
        download_url = f"https://data.london.gov.uk/download/{dataset_name}/{resource_id}/{file_name}"

        async with session.get(download_url) as file_response:
            if file_response.status != 200:
                print(f"Failed to download file from {download_url}")
                return
            content = await file_response.read()

            if resource.get('format', '').lower() == 'spreadsheet':
                df = pd.read_excel(io.BytesIO(content))
            elif resource.get('format', '').lower() == 'csv':
                df = pd.read_csv(io.BytesIO(content))
            else:
                print(f"Unsupported file format for {file_name}")
                return

            output_file_name = f"{result.get('id')}_{resource.get('name', 'unnamed')}.parquet"
            file_path = os.path.join(output_dir, output_file_name)
            df.to_parquet(file_path)
            print(f"Successfully saved {output_file_name} locally")
    except Exception as e:
        print(f"Error processing {download_url}: {str(e)}")

if __name__ == "__main__":
    asyncio.run(london_datastore())
