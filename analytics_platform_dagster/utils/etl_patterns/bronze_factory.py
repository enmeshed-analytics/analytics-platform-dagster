from typing import Any, Callable, Dict, List, Optional, TypeVar
import polars as pl
import requests
import io
from pydantic import BaseModel, ValidationError
from dagster import AssetExecutionContext
from functools import wraps
from builtins import bytes

T = TypeVar('T', bound=BaseModel)

class BronzeETLBase:
    """Base class for Bronze ETL operations supporting both API and Excel data sources using Polars"""

    def __init__(self, url_key: str, asset_urls: Dict[str, str]):
        self.url_key = url_key
        self.asset_urls = asset_urls

    def _is_excel_url(self, url: str) -> bool:
        """Check if URL points to an Excel file"""
        return any(url.lower().endswith(ext) for ext in ['.xlsx', '.xls', '.xlsm'])

    def _process_excel_response(self, response: bytes, sheet_name: Optional[str] = None) -> Dict:
        """Process Excel file from response bytes using Polars"""
        try:
            # Create a BytesIO object from the response content
            excel_buffer = io.BytesIO(response)

            # Read Excel file into Polars DataFrame
            if sheet_name:
                df = pl.read_excel(excel_buffer, sheet_name=sheet_name)
            else:
                df = pl.read_excel(excel_buffer)

            # Convert to records format similar to API response
            records = df.to_dicts()

            # Wrap in a dict with 'items' key to match API structure if needed
            return {'items': records}

        except Exception as e:
            raise ValueError(f"Error processing Excel file: {str(e)}")

    def fetch_data(self, sheet_name: Optional[str] = None) -> Dict:
        """Fetch data from either API or Excel source

        Args:
            sheet_name: Optional name of Excel sheet to read. If None, reads first sheet.

        Returns:
            Dict containing the data, with consistent structure regardless of source
        """
        url = self.asset_urls.get(self.url_key)
        if url is None:
            raise ValueError(f"URL for {self.url_key} not found in asset_urls")

        response = requests.get(url)
        response.raise_for_status()

        if self._is_excel_url(url):
            return self._process_excel_response(response.content, sheet_name)
        else:
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
    sheet_name: Optional[str] = None,
):
    """Factory function for creating bronze assets with Excel support using Polars

    Args:
        url_key: Key to lookup URL in asset_urls
        asset_urls: Dictionary of URLs
        model: Optional Pydantic model for validation
        transform_func: Optional function to transform data
        wrap_items: Whether to wrap items for validation
        sheet_name: Optional name of Excel sheet to read
    """
    def decorator(func):
        @wraps(func)
        def wrapper(context: AssetExecutionContext) -> bytes:
            etl = BronzeETLBase(url_key, asset_urls)
            try:
                # Fetch data (now supports Excel with Polars)
                data = etl.fetch_data(sheet_name=sheet_name)

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

                # Create DataFrame using Polars
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
