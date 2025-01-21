## GDPR Obfuscator Project
GDPR Obfuscation tool that can be integrated as a library module into a Python codebase.

## Table of Contents
1. [About](#about)
2. [Assumptions](#assumptions)
3. [High-Level Desired Outcome](#high-level-desired-outcome)
4. [Installation and Instructions](#installation-and-instructions)
5. [Testing](#testing)
6. [Usage](#usage)
7. [PEP8 and Security](#pep8-and-security)
 

## About
The purpose of this project is to create a general-purpose tool to process data being ingested to AWS and intercept personally identifiable information (PII). All information stored by Northcoders data projects should be for bulk data analysis only. Consequently, there is a requirement under GDPR to ensure that all data containing information that can be used to identify an individual should be anonymised.

- This obfuscation tool can be integrated as a library module into a Python codebase.

- It is expected that the code will use the AWS SDK for Python (boto3).

- The library is suitable for deployment on a platform within the AWS ecosystem, such as EC2, ECS, or Lambda.

## Assumptions 
1. Data is stored in **CSV format** in S3. This tool uses External Python libralies:
- Boto3 for managing AWS resources
- Botocore for Error handling available witin AWS enviroment

2. Fields containing GDPR-sensitive data are known and will be supplied in advance

3. Data records will be supplied with a **primary key**.


## High-Level Desired Outcome

The tool should be invoked by sending a JSON string containing: 
- the S3 location of the required CSV file for obfuscation and
- the names of the fields that are required to be obfuscated

**Example Input:**
```json
{
"file_to_obfuscate": "s3://bucket_name/path_to_data/file.csv",
"pii_fields": ["name", "email_address"]
}
```

**Example Target CSV File:**

```
student_id,name,course,cohort,graduation_date,email_address
1234,'John Smith','Software','2024-03-31','j.smith@email.com'
```
**Example Obfuscated Output:**

```
student_id,name,course,cohort,graduation_date,email_address
1234,'***','Software','2024-03-31','***'
```

The output will be a byte-stream representation of the file, compatible with the boto3 S3 PutObject function.

## Installation and Instructions

### Pre-requisites
Python: Ensure you have installed latest python version. Check version using below command
```
python --version 
```
or
```
python3 --version 
```

### Set-up 

1. Run the following command to set up your **virtual environment** and install required dependencies:
    ```
    make requirements

    pip install -r ./requirements.txt
    ```

2. Run this command next to set up security and coverage modules:
    ```
    make dev-setup
    ```

3. Set up your PYTHONPATH:
    ```
    export PYTHONPATH=$(pwd)
    ```

4. Run checks for unit-tests, pep8 compliancy, coverage and security vulnerabilities:
    ```
    make run-checks
    ```

## Testing 

To run unit tests run:
```
make unit-test
```

## Usage

clone the repo:
``` 
git clone https://github.com/laxmiprasannaimmadi/gdpr_obfuscator
```

Import:
```
from src.obfuscator import obfuscator
```

## PEP8 and Security

Code is written in Python, PEP8 compliant, tested with flake8.

As well as tested for security vulnerabilities: dependency vulnerability safety, security issues bandit.