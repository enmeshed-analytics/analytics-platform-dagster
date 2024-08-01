
from pydantic import ValidationError
from dagster import asset, op, AssetExecutionContext

from ...utils.requests_helper.requests_helper import return_json
from ...models.trade_data_models.trade_barriers_model import TradingBarriers
from ...utils.variables_helper.url_links import asset_urls

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

@asset(
    group_name="trade_assets", 
    io_manager_key="S3Json"
    )
def dbt_trade_barriers_bronze(context: AssetExecutionContext):
    """
    Load data into bronze bucket
    """
    try:
        url = asset_urls.get("dbt_trading_bariers_asset")
        data = return_json(url)
        validate_model(data)
        context.log.info("Model Validation Successful")
        return data
    except Exception as error:
        print(f"Error in dbt_trade_barriers: {str(error)}")
        raise error

