import polars as pl
import requests
import io
from typing import Any, Callable, Dict, List, Optional, TypeVar
from pydantic import BaseModel, ValidationError
from dagster import AssetExecutionContext
from functools import wraps
from builtins import bytes

T = TypeVar('T', bound=BaseModel)

class BronzeETLBase:
    """Base class for Bronze ETL operations"""
    def __init__(self, url_key: str, asset_urls: Dict[str, str]):
        self.url_key = url_key
        self.asset_urls = asset_urls

    def fetch_api_data(self) -> Dict:
        url = self.asset_urls.get(self.url_key)
        if url is None:
            raise ValueError(f"URL for {self.url_key} not found in asset_urls")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def validate_data(
        self,
        data: Any,
        model: Optional[type[T]] = None,
        wrap_items: bool = False
    ) -> List[Dict[str, Any]]:
        if model is None:
            return []

        try:
            if wrap_items:
                model.model_validate({"items": data})
            else:
                model.model_validate(data)
            return []
        except ValidationError as e:
            return [{"loc": str(err["loc"]), "msg": str(err["msg"])} for err in e.errors()]

    def to_parquet_bytes(self, df: pl.DataFrame) -> bytes:
        buf = io.BytesIO()
        df.write_parquet(buf)
        buf.seek(0)
        return buf.getvalue()

def bronze_asset_factory(
    url_key: str,
    asset_urls: Dict[str, str],
    model: Optional[type[T]] = None,
    transform_func: Optional[Callable] = None,
    wrap_items: bool = False,
):
    """Factory function for creating bronze assets

    Args:
        url_key: Key to lookup URL in asset_urls
        asset_urls: Dictionary of URLs
        model: Optional Pydantic model for validation
        transform_func: Optional function to transform data
        wrap_items: Whether to wrap items for validation
    """
    def decorator(func):
        @wraps(func)
        def wrapper(context: AssetExecutionContext) -> bytes:
            etl = BronzeETLBase(url_key, asset_urls)
            try:
                # Fetch data
                data = etl.fetch_api_data()

                # Validate data only if model is provided
                validation_errors = etl.validate_data(data, model, wrap_items) if model else []

                # Log validation errors if any
                if validation_errors:
                    for error in validation_errors:
                        context.log.warning(f"Validation error at {error['loc']}: {error['msg']}")
                context.log.info(f"Validation completed with {len(validation_errors)} errors")

                # Allow for custom transformations if required
                if transform_func:
                    transformed_data = transform_func(data)
                else:
                    transformed_data = data

                # Create DataFrame
                df = pl.DataFrame(transformed_data)

                # Log some info about the data
                context.log.info(f"Processed {len(df)} records")
                context.log.info(f"Preview: {(df).head(15)}")

                # Convert DataFrame to Parquet bytes
                parquet_bytes = etl.to_parquet_bytes(df)
                context.log.info("Byte Conversion Successful")

                # Return the Parquet bytes directly
                return parquet_bytes

            except Exception as e:
                context.log.error(f"Error in bronze asset: {str(e)}")
                raise

        return wrapper
    return decorator
