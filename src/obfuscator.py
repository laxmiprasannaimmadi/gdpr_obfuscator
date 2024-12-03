from urllib.parse import urlparse
import logging

def obfuscator(file_path):
    '''
    Retrieve data being ingested to AWS and intercept personally identifiable information (PII).

    Return byte-stream object containing an exact copy of the input file but with the sensitive data replaced with obfuscated strings.

    '''
    logger = logging.getLogger("Obfuscation process")
    logging.basicConfig()
    logger.setLevel(logging.INFO)

    logger.info("GDPR Obfuscation process started")
    pass

def get_bucket_key(s3_file_path):
    '''
    Extract S3 bucket name and key of the object from s3 file path
    Example: s3 file path: 's3://my_ingestion_bucket/new_data/file1.csv'    
    '''

    o = urlparse(s3_file_path, allow_fragments=False)
    return o.netloc, o.path.lstrip('/')

