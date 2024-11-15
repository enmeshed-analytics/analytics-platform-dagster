import polars as pl
import io

from ...utils.variables_helper.url_links import asset_urls
from ...utils.requests_helper.requests_helper import return_json
from ...models.environment_data_models.ea_flood_public_forecast_model import FloodRiskData
from ...utils.slack_messages.slack_message import with_slack_notification
from dagster import asset, AssetIn, AssetExecutionContext
from pydantic import ValidationError


@asset(group_name="environment_data", io_manager_key="S3Parquet")
def ea_flood_public_forecast_bronze(context: AssetExecutionContext):
    """
    EA Public Forecast flooding data bronze bucket
    """

    url = asset_urls.get("ea_flood_public_forecast")
    if url is None:
        raise ValueError("No url!")

    try:
        data = return_json(url)

        # Validate data against model
        validation_errors = []
        try:
            FloodRiskData.model_validate(data)
        except ValidationError as e:
            validation_errors = e.errors()

        df = pl.DataFrame(data)

        context.log.info(f"Processed {len(df)} records with {len(validation_errors)} validation errors")
        context.log.info(f"{df.head(5)}")

        parquet_buffer = io.BytesIO()
        df.write_parquet(parquet_buffer)
        parquet_bytes = parquet_buffer.getvalue()

        context.log.info("Successfully processed batch into Parquet format")
        return parquet_bytes
    except Exception as error:
        raise error


@asset(
    group_name="environment_data",
    io_manager_key="PolarsDeltaLake",
    metadata={"mode": "overwrite"},
    ins={"ea_flood_public_forecast_bronze": AssetIn("ea_flood_public_forecast_bronze")},
    required_resource_keys={"slack"}
)
@with_slack_notification("Environment Agency Public Flood Forecast Data")
def ea_flood_public_forecast_silver(context: AssetExecutionContext, ea_flood_public_forecast_bronze):
    """
    EA Public Forecast flooding data silver bucket - flattens nested JSON structure
    """
    # Extract the statement data
    statement = ea_flood_public_forecast_bronze['statement']

    # Create base output dictionary with top-level fields
    output = {
        "area_of_concern_url": statement.get('area_of_concern_url'),
        "headline": statement.get('headline'),
        "pdf_url": statement.get('pdf_url'),
        "issued_at": statement.get('issued_at'),
        "next_issue_due_at": statement.get('next_issue_due_at'),
    }

    # Add flood risk trend data
    risk_trend = statement.get('flood_risk_trend', {})
    output.update({
        "flood_risk_day1": risk_trend.get('day1'),
        "flood_risk_day2": risk_trend.get('day2'),
        "flood_risk_day3": risk_trend.get('day3'),
        "flood_risk_day4": risk_trend.get('day4'),
        "flood_risk_day5": risk_trend.get('day5')
    })

    # Add public forecast data
    public_forecast = statement.get('public_forecast', {})
    output.update({
        "england_forecast": public_forecast.get('england_forecast'),
        "wales_forecast_english": public_forecast.get('wales_forecast_english')
    })

    # Add source data
    for source in statement.get('sources', []):
        if 'coastal' in source:
            output['coastal_risk'] = source['coastal']
        if 'surface' in source:
            output['surface_risk'] = source['surface']
        if 'river' in source:
            output['river_risk'] = source['river']
        if 'ground' in source:
            output['ground_risk'] = source['ground']

    # Add risk areas
    output['risk_areas'] = statement.get('risk_areas')

    # Convert to DataFrame
    df = pl.DataFrame([output])

    return df
