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
    # Get the statement data
    statement = ea_flood_public_forecast_bronze.select('statement').item()

    # Create base output dictionary with top-level fields
    output = {
        "area_of_concern_url": statement['area_of_concern_url'] or "",  # Convert None to empty string
        "headline": statement['headline'] or "",
        "pdf_url": statement['pdf_url'] or "",
        "issued_at": statement['issued_at'] or "",
        "next_issue_due_at": statement['next_issue_due_at'] or "",
    }

    # Add flood risk trend data
    risk_trend = statement['flood_risk_trend']
    output.update({
        "flood_risk_day1": risk_trend['day1'] or "",
        "flood_risk_day2": risk_trend['day2'] or "",
        "flood_risk_day3": risk_trend['day3'] or "",
        "flood_risk_day4": risk_trend['day4'] or "",
        "flood_risk_day5": risk_trend['day5'] or ""
    })

    # Add public forecast data
    public_forecast = statement['public_forecast']
    output.update({
        "england_forecast": public_forecast['england_forecast'] or "",
        "wales_forecast_english": public_forecast['wales_forecast_english'] or ""
    })

    # Initialize source fields with empty strings
    output.update({
        "coastal_risk": "",
        "surface_risk": "",
        "river_risk": "",
        "ground_risk": ""
    })

    # Add source data
    for source in statement['sources']:
        if source.get('coastal'):
            output['coastal_risk'] = source['coastal']
        if source.get('surface'):
            output['surface_risk'] = source['surface']
        if source.get('river'):
            output['river_risk'] = source['river']
        if source.get('ground'):
            output['ground_risk'] = source['ground']

    # Convert risk areas to string to avoid nested structure issues
    risk_areas = statement.get('risk_areas', [])
    output['risk_areas'] = str(risk_areas) if risk_areas else ""

    # Convert to DataFrame and ensure string types for all columns
    df = pl.DataFrame([output]).with_columns([
        pl.col('*').cast(pl.String)
    ])

    return df
