import pandas as pd

from pydantic import ValidationError
from dagster import asset, op, AssetIn, AssetExecutionContext

from ...utils.requests_helper.requests_helper import return_json
from ...models.trade_data_models.trade_barriers_model import TradingBarriers
from ...utils.variables_helper.url_links import asset_urls
from ...utils.slack_messages.slack_message import with_slack_notification

API_ENDPOINT = asset_urls.get("dbt_trading_bariers_asset")

@op
def validate_model(trade_barriers_data) -> None:
    """Validate json against pydantic model"""
    try:
        TradingBarriers.model_validate(trade_barriers_data)
    except ValidationError as e:
        print("Validation errors:")
        for error in e.errors():
            print(f"Field: {error['loc']}, Error: {error['msg']}")
        raise


@asset(group_name="trade_assets", io_manager_key="S3Json")
def dbt_trade_barriers_bronze(context: AssetExecutionContext):
    """Load data into bronze bucket"""

    # Fetch url
    url = asset_urls.get("dbt_trading_bariers_asset")

    if url:
        try:
            data = return_json(url)
            validate_model(data)
            context.log.info("Model Validation Successful")
            return data
        except Exception as error:
            print(f"Error in dbt_trade_barriers: {str(error)}")
            raise error


@asset(
    group_name="trade_assets",
    io_manager_key="DeltaLake",
    metadata={"mode": "overwrite"},
    ins={"dbt_trade_barriers_bronze": AssetIn("dbt_trade_barriers_bronze")},
    required_resource_keys={"slack"}
)
@with_slack_notification("DBT Trade Barriers Data")
def dbt_trade_barriers_silver(
    context: AssetExecutionContext, dbt_trade_barriers_bronze
):
    """Load data into silver bucket"""
    data = dbt_trade_barriers_bronze
    trading_barriers = TradingBarriers.model_validate(data)

    flattened_data = []

    for barrier in trading_barriers.barriers:
        barrier_dict = barrier.model_dump()
        country = barrier_dict.pop("country")
        trading_bloc = country.pop("trading_bloc", None)
        sectors = barrier_dict.pop("sectors")
        barrier_dict.pop("categories", None)

        # Flatten country data
        barrier_dict.update(country)

        # Flatten trading bloc data
        if trading_bloc:
            for key, value in trading_bloc.items():
                if key != "overseas_regions":
                    barrier_dict[f"trading_bloc_{key}"] = value

            # Handle overseas regions
            if "overseas_regions" in trading_bloc:
                overseas_regions = trading_bloc["overseas_regions"]
                barrier_dict["trading_bloc_overseas_regions"] = [
                    region["name"] for region in overseas_regions
                ]
                barrier_dict["trading_bloc_overseas_region_ids"] = [
                    region["id"] for region in overseas_regions
                ]

        # Keep sectors as a list
        barrier_dict["sectors"] = [sector["name"] for sector in sectors]

        flattened_data.append(barrier_dict)

    # Create df
    df = pd.DataFrame(flattened_data)

    # Minor datatype transformations
    df = df.astype(str)
    df = df.fillna("N/A")
    time_date_cols = ["last_published_on", "reported_on"]

    for col in time_date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    # Log information
    context.log.info(f"Model Validation Successful {df.dtypes}")
    context.log.info(f"Model Validation Successful {df.head(15)}")

    return df
