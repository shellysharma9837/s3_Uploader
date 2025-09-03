S3_CONFIG = {
    "aws_access_key_id": "",
    "aws_secret_access_key": "",
    "region_name": "",
    "bucket_name": ""
}

# Columns that contain PII to mask
PII_COLUMNS = []

# Log file name
LOG_FILE = "/tmp/s3_upload.log"

# Retry attempts
RETRY_ATTEMPTS = 3

ENVIRONMENT = "dev"
TASK_NAME = "Data_processing"