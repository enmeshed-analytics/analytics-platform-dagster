import asyncio
import aiohttp
import pandas as pd
import io
import aioboto3
import os
from botocore.exceptions import ClientError

# The analytics_platform_london_datastore_test in the ideas dir works fine
# This version where it pushes to s3 needs testing before we make it a Dagster asset etc


async def london_datastore():
    """
    Fetch data from London Datastore API, process files, and upload as parquet to S3.
    """
    s3_bucket = 'your-s3-bucket-name'

    try:
        num_rows = 20
        url = f"https://data.london.gov.uk/api/action/package_search?rows={num_rows}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to fetch data: Status code {response.status}")
                data = await response.json()
            results = data['result']['result']
            tasks = []
            for result in results:
                for resource in result.get('resources', []):
                    if resource.get('format', '').lower() in ['spreadsheet', 'csv']:
                        task = asyncio.create_task(process_file(session, result, resource))
                        tasks.append(task)
            await asyncio.gather(*tasks)
        print(f"Finished processing all files. Output saved in S3 bucket: {s3_bucket}")
    except Exception as e:
        print(f"An error occurred in london_datastore: {str(e)}")
        raise

async def process_file(session, result, resource):
    """
    Process individual files using async and upload as parquet to S3 bucket.
    """

    # S3 configuration
    s3_bucket = 'your-s3-bucket-name'
    aws_region = 'your-aws-region'

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

            # Save DataFrame to parquet format in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer)
            parquet_buffer.seek(0)

            # Upload to S3
            async with aioboto3.Session().client('s3', region_name=aws_region) as s3_client:
                await s3_client.upload_fileobj(parquet_buffer, s3_bucket, output_file_name)

            print(f"Successfully uploaded {output_file_name} to S3 bucket {s3_bucket}")
    except ClientError as e:
        print(f"An error occurred while uploading to S3: {str(e)}")
    except Exception as e:
        print(f"Error processing {download_url}: {str(e)}")

if __name__ == "__main__":
    asyncio.run(london_datastore())
