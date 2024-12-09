from urllib.parse import urlparse
import logging
import json
import botocore

def obfuscator(file_path):
    '''
    Retrieve data being ingested to AWS and intercept personally identifiable information (PII).

    Return byte-stream object containing an exact copy of the input file but with the sensitive data replaced with obfuscated strings.

    "file_to_obfuscate" key:
        the S3 location of the required file for obfuscation
    "pii_fields" key:
        the list with names of the fields that are required to be obfuscated

    example:
    {
        "file_to_obfuscate": "s3://my_ingestion_bucket/new_data/file1.csv",
        "pii_fields": ["name", "email_address"]
    }
    
    return: bytestream representation of a file with obfuscated data fields

    '''
    logger = logging.getLogger("Obfuscation process")
    logging.basicConfig()
    logger.setLevel(logging.INFO)

    logger.info("GDPR Obfuscation process started")

    pydict = json.loads(file_path)
    bucket, key = get_bucket_and_key(pydict['file_to_obfuscate'])
    data_type = get_data_type(key)

    pass

def get_bucket_and_key(s3_file_path):
    '''
    Extract S3 bucket name and key of the object from s3 file path
    Example: s3 file path: 's3://my_ingestion_bucket/new_data/file1.csv'   

    return:  returns a tuple with (bucket,key)  --> ('my_ingestion_bucket', 'new_data/file1.csv')
        bucket: 'my_ingestion_bucket'
        key: 'new_data/file1.csv'
    '''

    o = urlparse(s3_file_path, allow_fragments=False)
    return o.netloc, o.path.lstrip('/')

# user-defined exception
class InvalidDataType(Exception):
    """Customized exception to raise if the data type is not in allowed data types"""
    pass

def get_data_type(key):
    '''
    Get data type from s3 object key
    Valid data types: csv, json, parquet
    '''
    data_types_allowed = ['csv','json','parquet']
    data_type = key.split('.')[-1]

    if data_type not in data_types_allowed:
        raise InvalidDataType(
            f' Supported data types are {",".join(data_types_allowed)} only'
        )

    return data_type

def get_data(client: botocore.client, )