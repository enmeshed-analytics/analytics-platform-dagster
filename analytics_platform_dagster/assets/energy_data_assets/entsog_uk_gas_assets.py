import requests
import pandas as pd

from pydantic import ValidationError
from ...models.energy_data_models.entsog_gas_data_assets_model import EntsogModel
from dagster import AssetExecutionContext, AssetIn, asset, op
from datetime import datetime, timedelta
from ...utils.slack_messages.slack_message import with_slack_notification


@op
def validate_model(entsog_data):
    """
    Validate json against pydantic model
    """
    try:
        EntsogModel.model_validate({"data": entsog_data}, strict=True)
    except ValidationError as e:
        print("Validation errors:")
        for error in e.errors():
            print(f"Field: {error['loc']}, Error: {error['msg']}")
        raise


@asset(group_name="energy_assets", io_manager_key="S3Json")
def entsog_gas_uk_data_bronze(context: AssetExecutionContext):
    """
    Put data in Bronze bucket
    """
    # Base URL
    base_url = "https://transparency.entsog.eu/api/v1/operationalData.json"

    # Parameters that don't change - this will get you gas flows in and out of the UK
    params = {
        "pointDirection": "UK-TSO-0003ITP-00005exit,UK-TSO-0001ITP-00005entry,IE-TSO-0002ITP-00495exit,UK-TSO-0001ITP-00090entry,UK-LSO-0001LNG-00008exit,UK-TSO-0001LNG-00008entry,UK-LSO-0002LNG-00049exit,UK-TSO-0001LNG-00049entry,UK-LSO-0004LNG-00049exit,UK-TSO-0002ITP-00496exit,UK-TSO-0002ITP-00077exit,IE-TSO-0001ITP-00077entry,UK-TSO-0004ITP-00207exit,UK-TSO-0001ITP-00207entry,UK-TSO-0001PRD-00156entry,UK-TSO-0001PRD-00151entry,UK-TSO-0001PRD-00152entry,UK-TSO-0001LNG-00007entry,UK-TSO-0001ITP-00284entry,UK-SSO-0007UGS-00419exit,UK-TSO-0001LNG-00053entry,UK-SSO-0012UGS-00275exit,UK-TSO-0001UGS-00275entry,UK-TSO-0001UGS-00414entry,UK-TSO-0001UGS-00276entry,UK-TSO-0001UGS-00277entry,UK-SSO-0014UGS-00404exit,UK-TSO-0001UGS-00404entry,UK-LSO-0003UGS-00278exit,UK-TSO-0001UGS-00278entry,UK-SSO-0009UGS-00279exit,UK-TSO-0001UGS-00279entry,UK-SSO-0007UGS-00421exit,UK-TSO-0001LNG-00054entry,UK-SSO-0008UGS-00420exit,UK-TSO-0001UGS-00280entry,UK-TSO-0001ITP-00279entry,UK-SSO-0005UGS-00178exit,UK-TSO-0001UGS-00281entry,UK-SSO-0001UGS-00181exit,UK-TSO-0001UGS-00181entry,UK-SSO-0009UGS-00423exit,UK-TSO-0001UGS-00282entry,UK-SSO-0007UGS-00422exit,UK-TSO-0001LNG-00055entry,UK-TSO-0001UGS-00235entry,UK-SSO-0003UGS-00216exit,UK-TSO-0001UGS-00216entry,UK-SSO-0016UGS-00289exit,UK-TSO-0001UGS-00289entry,NO-TSO-0001ITP-00091exit,UK-TSO-0001ITP-00091entry,NO-TSO-0001ITP-00022exit,UK-TSO-0001ITP-00022entry,UK-TSO-0001ITP-00436entry,UK-TSO-0001ITP-00492exit,UK-TSO-0001ITP-00492entry,UK-TSO-0001ITP-00090exit,IE-TSO-0001ITP-00496entry,UK-TSO-0001VTP-00006exit,UK-TSO-0001VTP-00006entry",
        "indicator": "Physical Flow",
        "periodType": "day",
        "timezone": "CET",
        "limit": -1,
        "dataset": 1,
        "formatType": "json",
    }

    # Calculate date range for API call
    today = datetime.now().date()
    week_ago = today - timedelta(days=7)

    # Add date parameters to the API call
    params["from"] = week_ago.strftime("%Y-%m-%d")
    params["to"] = today.strftime("%Y-%m-%d")

    try:
        # Make the request
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        # Parse JSON and validate model
        data = response.json()
        validated_data = data["operationalData"]
        validate_model(validated_data)

        context.log.info(f"Success: {validated_data}")

        return validated_data
    except requests.RequestException as e:
        # Handle any requests-related errors
        print(f"Error fetching data: {e}")
        raise
    except KeyError as e:
        # Handle missing key in the JSON response
        print(f"Error parsing JSON response: {e}")
        raise
    except ValidationError as e:
        # Handle validation errors
        print(f"Data validation error: {e}")
        raise
    except Exception as e:
        # Handle any other unexpected errors
        print(f"Unexpected error: {e}")
        raise


@asset(
    group_name="energy_assets",
    io_manager_key="DeltaLake",
    metadata={"mode": "overwrite"},
    ins={"entsog_gas_uk_data_bronze": AssetIn("entsog_gas_uk_data_bronze")},
    required_resource_keys={"slack"}
)
@with_slack_notification("ENTSOG Gas UK data")
def entsog_gas_uk_data_silver(
    context: AssetExecutionContext, entsog_gas_uk_data_bronze
):
    """
    Dump data into Silver bucket
    """

    # Access data in the Bronze delta lake
    data = entsog_gas_uk_data_bronze

    if data:
        try:
            df = pd.DataFrame(data)
            df = df.astype(str)
            context.log.info(f"Success: {df.head(25)}")
            context.log.info(f"Success: {df.columns}")
            return df
        except Exception as e:
            raise e
