import asyncio
import aiohttp
import pandas as pd
import io
import aioboto3
from dagster import asset, AssetExecutionContext

async def process_excel_file(context, session, s3, result, resource, bucket_name):
    """
    Process an individual Excel file and upload to S3.
    """
    try:
        excel_url = resource.get('url')
        async with session.get(excel_url) as excel_response:
            if excel_response.status != 200:
                context.log.warning(f"Failed to download Excel file from {excel_url}")
                return
            content = await excel_response.read()
            df = pd.read_excel(io.BytesIO(content))
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer)
            parquet_buffer.seek(0)
            file_name = f"{result.get('id')}_{resource.get('name', 'unnamed')}.parquet"
            await s3.upload_fileobj(parquet_buffer, bucket_name, file_name)
            context.log.info(f"Successfully uploaded {file_name} to S3")
    except Exception as e:
        context.log.error(f"Error processing {excel_url}: {str(e)}")

@asset
async def london_datastore(context: AssetExecutionContext):
    """
    Fetch data from London Datastore API, process Excel files, and upload to S3 as parquet.
    Need to test this locally first.
    """
    try:
        # Could lower this to 50 for testing?
        num_rows = 1000
        url = f"https://data.london.gov.uk/api/action/package_search?rows={num_rows}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch data: Status code {response.status}")
                data = await response.json()
        results = data['result']['result']

        bucket_name = 'your-s3-bucket-name'
        session = aioboto3.Session()
        async with session.client("s3") as s3:
            tasks = []
            for result in results:
                for resource in result.get('resources', []):
                    if resource.get('format', '').lower() in ['xlsx']:
                        task = asyncio.create_task(process_excel_file(context, session, s3, result, resource, bucket_name))
                        tasks.append(task)
            await asyncio.gather(*tasks)
        context.log.info("Finished processing all Excel files")
    except Exception as e:
        context.log.error(f"An error occurred in london_datastore: {str(e)}")
        raise
