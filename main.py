import requests
import pandas as pd
from s3_uploader import S3Uploader
from config import S3_CONFIG, PII_COLUMNS, LOG_FILE, RETRY_ATTEMPTS

def fetch_data_from_api():
    """
    Example: Fetch random dog images from Dog API
    Returns a DataFrame
    """
    url = "https://dog.ceo/api/breeds/image/random/5"  # 5 random dogs
    response = requests.get(url)
    response.raise_for_status()  # Raise error if request fails
    data = response.json()
    
    # Convert JSON to DataFrame
    df = pd.DataFrame(data["message"], columns=["image_url"])
    return df

def main():
    # Fetch data from API
    df = fetch_data_from_api()
    
    # Initialize S3 uploader
    uploader = S3Uploader()
    
    # Upload to S3 (append mode)
    uploader.upload_parquet(df, "demo/api_data.parquet", mode="append")

if __name__ == "__main__":
    main()
