import pytest
from unittest.mock import patch, MagicMock
from src.obfuscator import get_bucket_and_key, get_data, get_data_type,obfuscator, obfuscate_csv, InvalidDataType
from botocore.exceptions import ClientError

