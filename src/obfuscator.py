from urllib.parse import urlparse
from io import StringIO
import logging
import json
import botocore.session
import csv


def obfuscator(file_path):
    """
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

    """
    logger = logging.getLogger("Obfuscation process")
    logging.basicConfig()
    logger.setLevel(logging.INFO)

    logger.info("GDPR Obfuscation process started")

    pydict = json.loads(file_path)
    bucket, key = get_bucket_and_key(pydict["file_to_obfuscate"])
    data_type = get_data_type(key)

    #initiating s3 client 
    s3 = init_s3_client()

    #getting csv file data from s3 bucket in the form of bytes object and decoding it 
    input_data = get_data(s3, bucket, key).decode("utf-8")

    if data_type == "csv":
        pii_masked = obfuscate_csv(input_data, pydict["pii_fields"])

    return pii_masked


def get_bucket_and_key(s3_file_path):
    """
    Extract S3 bucket name and key of the object from s3 file path
    Example: s3 file path: 's3://my_ingestion_bucket/new_data/file1.csv'

    return:  returns a tuple with (bucket,key)  --> ('my_ingestion_bucket', 'new_data/file1.csv')
        bucket: 'my_ingestion_bucket'
        key: 'new_data/file1.csv'
    """

    o = urlparse(s3_file_path, allow_fragments=False)
    return o.netloc, o.path.lstrip("/")


# user-defined exception
class InvalidDataType(Exception):
    """Customized exception to raise if the data type is not in allowed data types"""

    pass


def get_data_type(key):
    """
    Get data type from s3 object key
    Valid data types: csv, json, parquet
    """
    print("inside get_data_type")
    data_types_allowed = ["csv", "json", "parquet"]
    data_type = key.split(".")[-1]

    if data_type not in data_types_allowed:
        raise InvalidDataType(
            f' Supported data types are {",".join(data_types_allowed)} only'
        )

    return data_type


def init_s3_client():
    """
    Initialises an s3 client using boto3.

            Parameters:
                    No inputs are taken for this function.

            Returns:
                    An instance of s3 client.
    """
    try:
        session = botocore.session.get_session()
        s3_client = session.create_client("s3")
        print("connected to s3")
        return s3_client

    except Exception:
        raise ConnectionRefusedError("Failed to connect to s3 client")


def get_data(client, bucket, key):
    """
    Retrieve data from s3
    client: s3 botocore client
    bucket: s3 bucket name
    key: s3 data key

    return: bytestream object representation of a data
    """
    print("inside get_data")
    logger = logging.getLogger(__name__)
    logging.basicConfig()
    logger.setLevel(logging.CRITICAL)

    logger.info("Retrieving data from s3")

    response = client.get_object(Bucket=bucket, Key=key)

    return response["Body"].read()


def obfuscate_csv(data, pii_fields):
    """
    Function that obfuscate/mask mentioned pii_fields in input file

    return:
        1. returns empty string, if input is empty
        2. if pii_field is not present in input data field names, function will send a warning and proceeds with other fields
        3. returns the masked csv file for pii_fields
    """
    print("inside obfuscate_csv")
    logger = logging.getLogger(__name__)
    logging.basicConfig()
    logger.setLevel(logging.WARNING)

    logger.info("obfuscate pii_fields in csv file")

    input_csv_dict = csv.DictReader(StringIO(data))

    print(f'input_csv_dict.fieldnames:', input_csv_dict.fieldnames)

    input_csv_dict.fieldnames = [x.lower() for x in input_csv_dict.fieldnames]
    print(f'input_csv_dict.fieldnames after lowercase:',input_csv_dict.fieldnames)

    if input_csv_dict.fieldnames is None:
        return str()

    masked_data = []
    for row in input_csv_dict:
        for field in pii_fields:
            if field in input_csv_dict.fieldnames :
                print(f'field:', field)
                row[field] = "***"
        masked_data.append(row)

    print(f'masked_data',masked_data)
    masked_bufer = StringIO()
    writer = csv.DictWriter(masked_bufer, input_csv_dict.fieldnames)
    writer.writeheader()
    writer.writerows(masked_data)

    print(f'masked_buffer:', masked_bufer.getvalue())
    return masked_bufer.getvalue()

obfuscator( 
    json.dumps(
        {
            'file_to_obfuscate': "s3://gdpr-obfuscator-data/new-data/file1.csv",
            'pii_fields': ["name", "email_address"]
        }
    ))