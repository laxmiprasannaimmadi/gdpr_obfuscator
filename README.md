## GDPR Obfuscator Project
GDPR Obfuscation tool that can be integrated as a library module into a Python codebase.

## Table of Contents
1. About
2. Assumptions
3. Usage
4. Installation Instructions
5. Prerequisites
6. Testing 
7. PEP8_and_security



## About
The purpose of this project is to create a general-purpose tool to process data being ingested to AWS and intercept personally identifiable information (PII). All information stored by Northcoders data projects should be for bulk data analysis only. Consequently, there is a requirement under GDPR to ensure that all data containing information that can be used to identify an individual should be anonymised.

This tool written in Python
It should be able to read file from AWS S3 using AWS SDK for Python(boto3)

## Assumptions 
1. Data is stored in CSV format in S3.
    This tool uses External Python libralies:
        :Boto3 for managing AWS resources
        :Botocore for Error handling available witin AWS enviroment

2. Fields containing GDPR-sensitive data are known and will be supplied in advance

3. Data records will be supplied with a primary key.

## Usage

clone the repo:

git clone https://github.com/laxmiprasannaimmadi/gdpr_obfuscator
Import:

from src.obfuscator import obfuscator


## Installation Instructions

## Prerequisites
    Python 3.x: ensure you have Python installed. Check version using
        
        python --version 
    
        or

        python3 --version 

## Set-up 

1. Run the following command to set up your virtual environment and install required dependencies:
    make requirements
        or
    for local run
        pip install -r ./requirements.txt

2. Run this command next to set up security and coverage modules:
    make dev-setup

3. Set up your PYTHONPATH:
    export PYTHONPATH=$(pwd)

4. Run checks for unit-tests, pep8 compliancy, coverage and security vulnerabilities:
    make run-checks

## Main funciton - Obfuscation

The main code 'obfuscator' will be supplied with a json file path containing the s3 location URL and the PII fields requiring obfuscating. 

## Testing 

To run unit tests run:

    make unit-test


The tool should be invoked by sending a JSON string containing: 
the S3 location of the required CSV file for obfuscation
and the names of the fields that are required to be obfuscated

JSON string format:
{
"file_to_obfuscate": "s3://bucket_name/path_to_data/file.csv",
"pii_fields": ["name", "surname", "other_filelds_to_mask"]
}

masked_data = obfuscator(file_path)

## PEP8_and_security

Code is written in Python,
PEP8 compliant, tested with flake8
As well as tested for security vulnerabilities:
dependency vulnerability safety, security issues bandit.