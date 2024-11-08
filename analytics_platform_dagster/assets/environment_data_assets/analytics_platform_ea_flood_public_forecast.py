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
    EA Public Forecast flooding data silver bucket
    """

    # Find data in bronze bucket
    data = ea_flood_public_forecast_bronze

    # Load data into model - to use dot notation further down
    flood_risk_data = FloodRiskData.model_validate(data)

    if flood_risk_data:
        statement = flood_risk_data.statement

        # Retrieve values using dot notation from the model
        output = {
            "area_of_concern": statement.area_of_concern_url,
            "headline": statement.headline,
            "pdf_report": statement.pdf_url,
            "issued_at": statement.issued_at,
            "next_issue_due": statement.next_issue_due_at,
            "flood_risk_day1": statement.flood_risk_trend.day1,
            "flood_risk_day2": statement.flood_risk_trend.day2,
            "flood_risk_day3": statement.flood_risk_trend.day3,
            "flood_risk_day4": statement.flood_risk_trend.day4,
            "flood_risk_day5": statement.flood_risk_trend.day5,
            "england_forecast": statement.public_forecast.england_forecast,
            "wales_forecast_english": statement.public_forecast.wales_forecast_english,
            "risks_areas": statement.risk_areas
        }

        # Access nested values
        for source in statement.sources:
            if source.coastal:
                output["coastal_risk"] = source.coastal
            if source.surface:
                output["surface_risk"] = source.surface
            if source.river:
                output["river_risk"] = source.river
            if source.ground:
                output["ground_risk"] = source.ground

        df = pl.DataFrame([output])

        return df
