"""Chargepoint Data for all of UK"""
import requests
import pyarrow as pa
import pandas as pd
import io

from typing import List, Dict, Any
from pydantic import ValidationError
from ...utils.variables_helper.url_links import asset_urls
from dagster import AssetExecutionContext, AssetIn, asset
from ...utils.slack_messages.slack_message import with_slack_notification
from ...models.infrastructure_data_models.national_charge_point_uk_model import (
    ChargeDeviceResponse,
)

# Create Arrow Schema creation of arrow table
def create_arrow_schema() -> pa.Schema:
    """
    Create Arrow schema for charge device data with all fields.

    Returns:
        pa.Schema: Complete Arrow schema matching all fields in the data
    """
    connector_type = pa.struct([
        ('ConnectorId', pa.string()),
        ('ConnectorType', pa.string()),
        ('ChargeMethod', pa.string()),
        ('ChargeMode', pa.string()),
        ('ChargePointStatus', pa.string()),
        ('RatedOutputkW', pa.float32()),
        ('RatedOutputVoltage', pa.string()),
        ('RatedOutputCurrent', pa.string()),
        ('TetheredCable', pa.bool_()),
        ('Validated', pa.bool_())
    ])

    return pa.schema([
        # Basic Information
        ('ChargeDeviceRef', pa.string()),
        ('ChargeDeviceId', pa.string()),
        ('ChargeDeviceName', pa.string()),
        ('ChargeDeviceStatus', pa.string()),
        ('ChargeDeviceManufacturer', pa.string()),
        ('ChargeDeviceModel', pa.string()),

        # Location Information
        ('Latitude', pa.string()),
        ('Longitude', pa.string()),
        ('LocationType', pa.string()),
        ('LocationDescription', pa.string()),

        # Address Fields
        ('BuildingName', pa.string()),
        ('BuildingNumber', pa.string()),
        ('Street', pa.string()),
        ('Thoroughfare', pa.string()),
        ('PostTown', pa.string()),
        ('County', pa.string()),
        ('PostCode', pa.string()),
        ('Country', pa.string()),

        # Controller/Owner Information
        ('ControllerName', pa.string()),
        ('ControllerContact', pa.string()),
        ('ControllerWebsite', pa.string()),
        ('OwnerName', pa.string()),

        # Access and Payment Information
        ('AccessRestrictionFlag', pa.bool_()),
        ('AccessRestrictionDetails', pa.string()),
        ('Accessible24Hours', pa.bool_()),
        ('PaymentRequiredFlag', pa.bool_()),
        ('PaymentDetails', pa.string()),
        ('ParkingFeesFlag', pa.bool_()),
        ('ParkingFeesDetails', pa.string()),

        # Network and Attribution
        ('DeviceNetworks', pa.string()),
        ('Attribution', pa.string()),

        # Status and Validation
        ('PublishStatus', pa.string()),
        ('DeviceValidated', pa.string()),

        # Dates
        ('DateCreated', pa.string()),
        ('DateUpdated', pa.string()),
        ('DateDeleted', pa.string()),

        # Connector Information
        ('Connectors', pa.list_(connector_type))
    ])

# Prepare a batch of data for arrow data creation
def prepare_batch_data(charge_devices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prepare batch data ensuring that all fields are present and properly formatted.

    Args:
        charge_devices: List of charge device dictionaries containing device information

    Returns:
        List of standardised charge device records with all relevant fields
    """
    prepared_data = []

    for device in charge_devices:
        try:
            # Extract nested structures
            location = device.get('ChargeDeviceLocation', {})
            address = location.get('Address', {})
            device_controller = device.get('DeviceController', {})
            device_owner = device.get('DeviceOwner', {})
            connectors = device.get('Connector', [])

            # Prepare base record with all fields
            record = {
                # Basic Information
                'ChargeDeviceRef': str(device.get('ChargeDeviceRef', '')),
                'ChargeDeviceId': str(device.get('ChargeDeviceId', '')),
                'ChargeDeviceName': str(device.get('ChargeDeviceName', '')).strip(),
                'ChargeDeviceStatus': str(device.get('ChargeDeviceStatus', '')),
                'ChargeDeviceManufacturer': str(device.get('ChargeDeviceManufacturer', '')),
                'ChargeDeviceModel': str(device.get('ChargeDeviceModel', '')),

                # Location Information
                'Latitude': str(location.get('Latitude', '')),
                'Longitude': str(location.get('Longitude', '')),
                'LocationType': str(device.get('LocationType', '')),
                'LocationDescription': str(location.get('LocationLongDescription', '')),

                # Address Fields
                'BuildingName': str(address.get('BuildingName', '')),
                'BuildingNumber': str(address.get('BuildingNumber', '')),
                'Street': str(address.get('Street', '')),
                'Thoroughfare': str(address.get('Thoroughfare', '')),
                'PostTown': str(address.get('PostTown', '')),
                'County': str(address.get('County', '')),
                'PostCode': str(address.get('PostCode', '')),
                'Country': str(address.get('Country', '')),

                # Controller/Owner Information
                'ControllerName': str(device_controller.get('OrganisationName', '')),
                'ControllerContact': str(device_controller.get('TelephoneNo', '')),
                'ControllerWebsite': str(device_controller.get('Website', '')),
                'OwnerName': str(device_owner.get('OrganisationName', '')),

                # Access and Payment Information
                'AccessRestrictionFlag': bool(device.get('AccessRestrictionFlag', False)),
                'AccessRestrictionDetails': str(device.get('AccessRestrictionDetails', '')),
                'Accessible24Hours': bool(device.get('Accessible24Hours', False)),
                'PaymentRequiredFlag': bool(device.get('PaymentRequiredFlag', False)),
                'PaymentDetails': str(device.get('PaymentDetails', '')),
                'ParkingFeesFlag': bool(device.get('ParkingFeesFlag', False)),
                'ParkingFeesDetails': str(device.get('ParkingFeesDetails', '')),

                # Network and Attribution
                'DeviceNetworks': str(device.get('DeviceNetworks', '')),
                'Attribution': str(device.get('Attribution', '')),

                # Status and Validation
                'PublishStatus': str(device.get('PublishStatus', '')),
                'DeviceValidated': str(device.get('DeviceValidated', '')),

                # Dates
                'DateCreated': str(device.get('DateCreated', '')),
                'DateUpdated': str(device.get('DateUpdated', '')),
                'DateDeleted': str(device.get('DateDeleted', '')),

                # Connector Information
                'Connectors': [
                    {
                        'ConnectorId': str(conn.get('ConnectorId', '')),
                        'ConnectorType': str(conn.get('ConnectorType', '')),
                        'ChargeMethod': str(conn.get('ChargeMethod', '')),
                        'ChargeMode': str(conn.get('ChargeMode', '')),
                        'ChargePointStatus': str(conn.get('ChargePointStatus', '')),
                        'RatedOutputkW': float(conn.get('RatedOutputkW', 0)),
                        'RatedOutputVoltage': str(conn.get('RatedOutputVoltage', '')),
                        'RatedOutputCurrent': str(conn.get('RatedOutputCurrent', '')),
                        'TetheredCable': bool(int(conn.get('TetheredCable', 0))),
                        'Validated': bool(int(conn.get('Validated', 0)))
                    }
                    for conn in connectors
                ]
            }

            prepared_data.append(record)

        except Exception as e:
            print(f"Error processing device {device.get('ChargeDeviceRef', 'Unknown')}: {str(e)}")
            continue

    return prepared_data

@asset(
    group_name="infrastructure_assets",
    io_manager_key="S3Parquet"
)
def national_charge_point_uk_bronze(context: AssetExecutionContext):
    """
    Write charge point data out to raw staging area

    Returns:
        Parquet in S3.
    """
    url = asset_urls.get("national_charge_point_uk")

    if url is None:
        raise ValueError("No url!")

    validation_errors = []
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        try:
            ChargeDeviceResponse.model_validate(data)
        except ValidationError as e:
            validation_errors = e.errors()

        prepared_data = prepare_batch_data(data['ChargeDevice'])

        df = pd.DataFrame(prepared_data)
        df = df.astype(str)
        context.log.info(f"Processed {len(df)} records with {len(validation_errors)} validation errors")

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow")
        parquet_bytes = parquet_buffer.getvalue()

        context.log.info("Successfully processed batch into Parquet format")
        return parquet_bytes

    except Exception as e:
        context.log.error(f"Error processing data: {str(e)}")
        raise e

@asset(
    group_name="infrastructure_assets",
    io_manager_key="DeltaLake",
    metadata={"mode": "overwrite"},
    ins={
        "national_charge_point_uk_bronze": AssetIn(
            "national_charge_point_uk_bronze"
        )
    },
    required_resource_keys={"slack"}
)
@with_slack_notification("National EV Charge Point Data UK")
def national_charge_point_uk_silver(
    context: AssetExecutionContext, national_charge_point_uk_bronze
) -> pd.DataFrame:
    """
    Write charge point data out to Delta Lake

    Returns:
        Delta Lake table in S3.
    """
    try:
        df = pd.DataFrame(national_charge_point_uk_bronze)
        return df

    except Exception as e:
        context.log.error(f"Error processing data: {e}")
        raise
