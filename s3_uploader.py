import boto3
import pandas as pd
import logging
import time
from config import *
from datetime import datetime

# Setup logging
current_time = datetime.now()
date_str = current_time.strftime("%Y-%m-%d")
time_str = current_time.strftime("%H%M%S")
LOG_FILE = f"/tmp/{TASK_NAME}_{time_str}.log"
S3_LOG_PATH = f"logs/{ENVIRONMENT}/{TASK_NAME}/{date_str}/{TASK_NAME}_{time_str}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

class S3Uploader:
    def __init__(self):
        """Initialize S3 client and bucket name"""
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=S3_CONFIG['aws_access_key_id'],
            aws_secret_access_key=S3_CONFIG['aws_secret_access_key'],
            region_name=S3_CONFIG['region_name']
        )
        self.bucket = S3_CONFIG['bucket_name']

    def mask_pii(self, df):
        """Mask PII columns"""
        for col in PII_COLUMNS:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: f"{str(x)[0]}***{str(x)[-1]}")
        return df

    def upload_parquet(self, df, path, mode="append"):
        """Upload DataFrame as parquet to S3 with append mode"""
        df = self.mask_pii(df)  # Mask PII
        filename = "/tmp/temp.parquet"

        # Retry logic
        for attempt in range(RETRY_ATTEMPTS):
            try:
                if mode == "append":
                    try:
                        existing_df = pd.read_parquet(f"s3://{self.bucket}/{path}", storage_options={
                            "key": S3_CONFIG['aws_access_key_id'],
                            "secret": S3_CONFIG['aws_secret_access_key']
                        })
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        combined_df.to_parquet(filename, index=False)
                        logging.info(f"Appended data to {path}")
                    except FileNotFoundError:
                        df.to_parquet(filename, index=False)
                        logging.info(f"File not found. Created new file {path}")
                else:
                    df.to_parquet(filename, index=False)
                    logging.info(f"Overwriting file {path}")

                # Upload to S3
                self.s3.upload_file(filename, self.bucket, path)
                logging.info(f"Uploaded {filename} to s3://{self.bucket}/{path}")

                # Governance: track metadata
                logging.info(f"Governance Info: Uploaded by S3Uploader, rows={len(df)}, columns={list(df.columns)}")

                # Upload logs to S3
                self.s3.upload_file(LOG_FILE, self.bucket, S3_LOG_PATH)
                logging.info(f"Log file uploaded to s3://{self.bucket}/{S3_LOG_PATH}/{LOG_FILE}")
                
                break  # success, break retry loop

            except Exception as e:
                logging.error(f"Attempt {attempt+1} failed: {e}")
                time.sleep(2)  # wait before retry
                if attempt == RETRY_ATTEMPTS - 1:
                    raise
