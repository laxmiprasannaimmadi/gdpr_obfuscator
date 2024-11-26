## GDPR Obfuscator Project
## About
The purpose of this project is to create a general-purpose tool to process data being ingested to AWS and intercept personally identifiable information (PII). All information stored by Northcoders data projects should be for bulk data analysis only. Consequently, there is a requirement under GDPR to ensure that all data containing information that can be used to identify an individual should be anonymised.

This tool should be written in Python
It should be able to read file from AWS S3 using AWS SDK for Python(boto3)


## Set-up 

1. Run the following command to set up your virtual environment and install required dependencies:
    make requirements

2. Run this command next to set up security and coverage modules:
    make dev-setup

3. Set up your PYTHONPATH:
    export PYTHONPATH=$(pwd)

4. Run checks for unit-tests, pep8 compliancy, coverage and security vulnerabilities:
    make run-checks

## Main funciton - Obfuscation

The main code 'obfuscator' will be supplied with a json file path containing the s3 location URL and the PII fields requiring obfuscating. 

## Testing 