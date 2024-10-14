# Enmeshed Data Lake

Data Lake setup for Enmeshed!

> [!NOTE]
> This is currently a work in progress.
>
> This project is in early stage development.
>
> Run dagster dev -m analytics_platform_dagster to develop locally.
>
> Or use the makefile and run make dagster-dev (you need to set up the .env file for this)

> [!IMPORTANT]
> To use Streamlit Data Lake Checker locally
>
> Create .streamlit dir and secrets.toml file in root dir (make sure to add them to gitignore)
>
> Add in bucket = "WHATEVER THE SILVER BUCKET URI IS IN S3"
>
> Ensure you run aws-vault exec <profile> so Boto3 has access to the creds
>
> Activate Python venv
>
> Run <Streamlit run streamlit_app/main.py>

## Current Architecture
![Untitled-2024-09-24-1622-2](https://github.com/user-attachments/assets/ba0350e4-a605-4a1b-bd9c-df302955d314)
