import polars as pl
from functools import wraps

def send_slack_silver_success_message(context, df, asset_name):
    """
    Function to send a message to Slack following the successful completion of an Asset.
    Sends formatted string containing df.schema and basic stats.
    """
    # Basic info
    basic_info = (
        f"📚 Schema:\n```\n{df.schema}\n```\n\n"
        f"🔸 Shape: {df.shape[0]:,} rows × {df.shape[1]} columns\n"
        f"💾 Memory usage: {df.estimated_size() / 1024 / 1024:.2f} MB\n\n"
    )

    # Combine all information
    message = (
        f"✅ *{asset_name}* successfully processed and stored in Silver Bucket.\n\n"
        f"{basic_info}"
    )

    # Send the message
    context.resources.slack.get_client().chat_postMessage(
        channel="#pipelines",
        text=message
    )

def with_slack_notification(asset_name):
    """
    Wrapper to create a slack message decorator for an asset.
    Works with Polars DataFrames.
    """
    def slack_df_success_decorator(func):
        @wraps(func)
        def wrapper(context, *args, **kwargs):
            result = func(context, *args, **kwargs)
            if isinstance(result, pl.DataFrame):
                send_slack_silver_success_message(context, result, asset_name)
            return result
        return wrapper
    return slack_df_success_decorator
