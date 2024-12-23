import pytest
import os
import json
import boto3
import sys
from io import StringIO
import csv
import time
import logging
from moto import mock_aws
from unittest.mock import patch, MagicMock
from src.obfuscator import (
    init_s3_client,
    get_bucket_and_key,
    get_data,
    get_data_type,
    obfuscator,
    obfuscate_file,
    InvalidDataType,
)
from botocore.exceptions import ClientError


@pytest.fixture(scope="class")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def mock_s3_client(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture
def csv_data():
    """
    :returns: (tuple[str, str]) csv data structured for boto3 put_object(),
        and expected csv data for assertion masking ['name', 'country']
    """
    csv_buffer = StringIO()
    headers = ["id", "name", "surname", "country"]
    data = [
        ["1", "test_name1", "test_surname1", "test_country1"],
        ["2", "test_name2", "test_surname2", "test_country2"],
    ]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)

    expected_output_headers = ["id", "name", "surname", "country"]
    expected_output_data = [
        ["1", "***", "test_surname1", "***"],
        ["2", "***", "test_surname2", "***"],
    ]
    expected_csv_buffer = StringIO()
    writer = csv.writer(expected_csv_buffer)
    writer.writerow(expected_output_headers)
    writer.writerows(expected_output_data)

    return csv_buffer.getvalue(), expected_csv_buffer.getvalue()


@pytest.fixture
def csv_data_capital_name_fields():
    """
    :returns: (tuple[str, str]) csv data structured for boto3 put_object(),
        and expected csv data for assertion masking ['name', 'country']
    """
    csv_buffer = StringIO()
    headers = ["ID", "NAME", "SURNAME", "COUNTRY"]
    data = [
        ["1", "test_name1", "test_surname1", "test_country1"],
        ["2", "test_name2", "test_surname2", "test_country2"],
    ]
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    writer.writerows(data)

    expected_output_headers = ["id", "name", "surname", "country"]
    expected_output_data = [
        ["1", "***", "test_surname1", "***"],
        ["2", "***", "test_surname2", "***"],
    ]
    expected_csv_buffer = StringIO()
    writer = csv.writer(expected_csv_buffer)
    writer.writerow(expected_output_headers)
    writer.writerows(expected_output_data)

    return csv_buffer.getvalue(), expected_csv_buffer.getvalue()


class TestGetBucketAndKey:
    @pytest.mark.describe("get_bucket_and_key")
    @pytest.mark.it("Test correct bucket and key are extracted from s3 file path")
    def test_correct_bucket_and_key_are_extracted_from_given_s3_location(self):
        s3_file_path = "s3://gdpr-obfuscator-data/new-data/file2.csv"
        bucket, key = get_bucket_and_key(s3_file_path)
        assert bucket == "gdpr-obfuscator-data"
        assert key == "new-data/file2.csv"

    @pytest.mark.describe("get_bucket_and_key")
    @pytest.mark.it(
        "Test the return values if invalid bucket and key given in s3 file path"
    )
    def test_the_return_values_if_invalid_bucket_and_key_given_in_s3_file_path(self):
        s3_file_path = ""
        bucket, key = get_bucket_and_key(s3_file_path)
        assert bucket == ""
        assert key == ""


class TestGetDataType:
    @pytest.mark.describe("get_data_type")
    @pytest.mark.it("Test data type is extracted correctly from the given key")
    def test_data_type_is_extracted_correctly_from_the_given_key(self):
        s3_file_path = "s3://gdpr-obfuscator-data/new-data/file2.csv"
        __, key = get_bucket_and_key(s3_file_path)
        actual_ouput = get_data_type(key)
        expected_output = "csv"
        assert actual_ouput == expected_output

    @pytest.mark.describe("get_data_type")
    @pytest.mark.it("Test exception is raised if the key doesn't have an extension")
    def test_exception_is_raised_if_the_key_doesnt_have_an_extension(self):
        s3_file_path = "s3://gdpr-obfuscator-data/new-data/file2"
        __, key = get_bucket_and_key(s3_file_path)
        with pytest.raises(Exception):
            get_data_type(key)

    @pytest.mark.describe("get_data_type")
    @pytest.mark.it(
        "Test InvalidDataType exception is raised if the data type is not in allowed data types"
    )
    def test_InvalidDataType_exception_is_raised_if_the_data_type_is_not_in_allowed_data_types(
        self,
    ):
        s3_file_path = "s3://gdpr-obfuscator-data/new-data/file2.txt"
        __, key = get_bucket_and_key(s3_file_path)
        with pytest.raises(InvalidDataType) as excp_text:
            get_data_type(key)

        assert "Supported data types are csv,json,parquet only" in str(excp_text.value)


class TestInitS3Client:
    @pytest.mark.describe("init_s3_client")
    @pytest.mark.it("Test s3 client connection success")
    @patch("botocore.session.get_session")
    def test_client_connection(self, mock_s3_client):
        mock_session_rv = mock_s3_client.return_value
        mock_client = mock_session_rv.create_client.return_value
        s3_client = init_s3_client()

        assert mock_session_rv.create_client.called_once_with("s3")
        assert s3_client == mock_client

    @pytest.mark.describe("init_s3_client")
    @pytest.mark.it("Test s3 client connection failure")
    @patch("botocore.session.get_session")
    def test_client_connection_failure(self, mock_s3_client):
        mock_session_rv = mock_s3_client.return_value
        mock_session_rv.create_client.side_effect = Exception

        with pytest.raises(ConnectionRefusedError):
            init_s3_client()


class TestGetData:
    @pytest.mark.describe("get_data")
    @pytest.mark.it("Test data is retrieved successfully from s3")
    def test_data_is_retrieved_successfully_from_s3(self, mock_s3_client, csv_data):
        s3_file_path = "s3://gdpr-obfuscator-data/new-data/file2.csv"
        bucket, key = get_bucket_and_key(s3_file_path)

        test_csv_data, __ = csv_data
        mock_s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_client.put_object(Bucket=bucket, Key=key, Body=test_csv_data)

        s3_data = get_data(mock_s3_client, bucket, key).decode()
        input_csv_dict = csv.reader(StringIO(s3_data))
        pylist = [row for row in input_csv_dict]
        assert pylist[0] == ["id", "name", "surname", "country"]
        assert pylist[1:] == [
            ["1", "test_name1", "test_surname1", "test_country1"],
            ["2", "test_name2", "test_surname2", "test_country2"],
        ]

    @pytest.mark.describe("get_data")
    @pytest.mark.it("Test to raise NoSuchKey error with wrong key")
    def test_to_raise_NoSuchKey_error_with_wrong_key(self, mock_s3_client, csv_data):
        s3_file_path = "s3://gdpr-obfuscator-data/new-data/file2.csv"
        bucket, key = get_bucket_and_key(s3_file_path)

        test_csv_data, __ = csv_data
        mock_s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_client.put_object(Bucket=bucket, Key=key, Body=test_csv_data)

        with pytest.raises(ClientError) as excinfo:
            get_data(mock_s3_client, bucket, "wrong_key")
        assert "NoSuchKey" in str(excinfo.value)

    @pytest.mark.describe("get_data")
    @pytest.mark.it("Test to raise NoSuchBacket error with wrong buket")
    def test_to_raise_NoSuchBacket_error_with_wrong_bucket(
        self, mock_s3_client, csv_data
    ):
        s3_file_path = "s3://gdpr-obfuscator-data/new-data/file2.csv"
        bucket, key = get_bucket_and_key(s3_file_path)

        test_csv_data, __ = csv_data
        mock_s3_client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_client.put_object(Bucket=bucket, Key=key, Body=test_csv_data)

        with pytest.raises(ClientError) as excinfo:
            get_data(mock_s3_client, "WRONG_BUCKET", key)
        assert "NoSuchBucket" in str(excinfo.value)


class TestObfuscateFile:
    @pytest.mark.describe("obfuscate_file")
    @pytest.mark.it("Test to check the function masks the correct fields")
    def test_to_check_the_function_masks_the_correct_fields(self, csv_data):
        test_csv_data, obfuscated_csv_data = csv_data
        pii_fields = ["name", "country"]
        masked_csv = obfuscate_file(test_csv_data, pii_fields)

        assert masked_csv == obfuscated_csv_data

    @pytest.mark.describe("obfuscate_file")
    @pytest.mark.it(
        "Test to check the function masks the correct fields and not case sensitive"
    )
    def test_to_check_the_function_masks_the_correct_fields_and_not_case_sensitive(
        self, csv_data_capital_name_fields
    ):
        test_csv_data, obfuscated_csv_data = csv_data_capital_name_fields
        pii_fields = ["name", "country"]
        masked_csv = obfuscate_file(test_csv_data, pii_fields)

        assert masked_csv == obfuscated_csv_data

    @pytest.mark.describe("obfuscate_file")
    @pytest.mark.it(
        "Test obfuscate function returns empty string when no data is passed"
    )
    def test_obfuscate_function_returns_empty_string_when_no_data_is_passed(self):
        masked_csv = obfuscate_file("", ["no_field"])
        assert masked_csv == ""

    @pytest.mark.describe("obfuscate_file")
    @pytest.mark.it("Test the function that skips pii field which is not in input_csv")
    def test_the_function_that_skips_pii_field_which_is_not_in_input_csv(
        self, csv_data
    ):
        test_csv_data, expected_csv_data = csv_data
        pii_fields = ["name", "country", "wrong_column_name"]
        masked_csv = obfuscate_file(test_csv_data, pii_fields)
        assert masked_csv == expected_csv_data

    @pytest.mark.describe("obfuscate_file")
    @pytest.mark.it("Test the function logs when pii field is not in input csv fields")
    def test_the_function_logs_when_pii_field_is_not_in_input_csv_fields(
        self, csv_data, caplog
    ):
        test_csv_data, _ = csv_data
        pii_fields = ["name", "country", "wrong_column_name"]
        obfuscate_file(test_csv_data, pii_fields)
        assert "WARNING" in caplog.text
        assert "pii_field:'wrong_column_name' not in data" in caplog.text


class TestObfuscator:
    @pytest.mark.describe("obfuscator")
    @pytest.mark.it(
        "Test the function returns data with correct pii_fields masked csv data"
    )
    def test_the_function_returns_data_with_correct_pii_fields_masked_csv_data(
        self, mock_s3_client, csv_data
    ):
        test_csv_data, expected_csv_data = csv_data
        s3_file = "s3://test_bucket/some_folder/file.csv"
        pii_fields = ["name", "country"]
        d = {}
        d["file_to_obfuscate"], d["pii_fields"] = s3_file, pii_fields
        json_str = json.dumps(d)
        mock_s3_client.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_client.put_object(
            Bucket="test_bucket", Key="some_folder/file.csv", Body=test_csv_data
        )

        masked_csv = obfuscator(json_str).decode()

        assert expected_csv_data == masked_csv

    @pytest.mark.describe("obfuscator")
    @pytest.mark.it(
        "Test the function that return bytestream representation of data csv data"
    )
    def test_the_function_that_return_bytestream_representation_of_data_csv_data(
        self, mock_s3_client, csv_data
    ):
        test_csv_data, expected_csv_data = csv_data
        s3_file = "s3://test_bucket/some_folder/file.csv"
        pii_fields = ["name", "country"]
        d = {}
        d["file_to_obfuscate"], d["pii_fields"] = s3_file, pii_fields
        json_str = json.dumps(d)
        mock_s3_client.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_client.put_object(
            Bucket="test_bucket", Key="some_folder/file.csv", Body=test_csv_data
        )

        masked_csv = obfuscator(json_str)
        assert isinstance(masked_csv, bytes)

    @pytest.mark.describe("obfuscator")
    @pytest.mark.it("Test the function process 1MB data in less than 1min")
    def test_the_function_process_1MB_data_in_less_than_1min(self, mock_s3_client):
        headers = ["id", "name", "surname", "country"]
        data = []
        counter = 1
        while sys.getsizeof(data) <= 1048576:
            data.append(
                [
                    str_counter := str(counter),
                    "test_name" + str_counter,
                    "test_surname" + str_counter,
                    "test_country" + str_counter,
                ]
            )
            counter += 1
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(headers)
        writer.writerows(data)
        csv_data = csv_buffer.getvalue()

        mock_s3_client.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_client.put_object(
            Bucket="test_bucket", Key="some_folder/file.csv", Body=csv_data
        )

        s3_file = "s3://test_bucket/some_folder/file.csv"
        pii_fields = headers
        d = {}
        d["file_to_obfuscate"], d["pii_fields"] = s3_file, pii_fields
        start_time = time.time()
        obfuscator(json.dumps(d))
        assert time.time() - start_time < 60
