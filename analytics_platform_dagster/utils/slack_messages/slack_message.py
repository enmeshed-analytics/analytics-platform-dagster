import pandas as pd
from functools import wraps
import io

def send_slack_silver_success_message(context, df, asset_name):
    """
    Function to send a message to Slack following the successful completion of an Asset.

    Sends formatted string containing df.info information.

    Leverages the AssetExcecuitonContext to access data directly - in this case a Pandas DataFrame.

    Args:
        context (AssetExecutionContext)
        df
        name of the asset

    """
    # Capture DataFrame info in a string buffer
    buffer = io.StringIO()
    df.info(buf=buffer)
    df_info = buffer.getvalue()

    # call the postMessage method.
    context.resources.slack.get_client().chat_postMessage(
        channel="#pipelines",
        text=f"{asset_name} successfully processed and stored in Silver Bucket.\n"
             f"Data Overview:\n```\n{df_info}\n```"
    )

def with_slack_notification(asset_name):
    """
    Wrapper to create a slack message decorator for an asset.

    This will only send a message if the return is successful.

    Otherwise the function completes and no message is sent.

    For example (not full code snippet):

    import pandas as pd
    from dagster import asset, AssetIn, AssetExecutionContext
    from ...utils.slack_messages.slack_message import with_slack_notification

        @asset(
            group_name="energy_assets",
            io_manager_key="DeltaLake",
            metadata={"mode": "overwrite"},
            ins={"entsog_gas_uk_data_bronze": AssetIn("entsog_gas_uk_data_bronze")},
            required_resource_keys={"slack"}
        )
        @with_slack_notification("ENTSOG Gas UK data")
        def entsog_gas_uk_data_silver(context: AssetExecutionContext, entsog_gas_uk_data_bronze):
            etc...

    """
    def slack_df_success_decorator(func):
        @wraps(func)
        def wrapper(context, *args, **kwargs):
            result = func(context, *args, **kwargs)
            if isinstance(result, pd.DataFrame):
                send_slack_silver_success_message(context, result, asset_name)
            return result
        return wrapper
    return slack_df_success_decorator
