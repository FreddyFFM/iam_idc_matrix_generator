# IAM Identity Center Permission Matrix Generator

A Python-based tool for analyzing and visualizing AWS IAM Identity Center (formerly AWS SSO) permissions across your organization. This tool helps you understand who has access to what across your AWS accounts.

## Features

- Extracts permission sets and their configurations from IAM Identity Center
- Analyzes inline and managed policies
- Creates interactive pivot tables for permission analysis
- Supports automatic processing of the latest data files
- Comprehensive logging of all operations

## Prerequisites

- Python 3.11 or higher
- AWS credentials configured with appropriate permissions
- Virtual environment (recommended)

## Required AWS Permissions

The following AWS permissions are required:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sso-admin:ListInstances",
                "sso-admin:ListPermissionSets",
                "sso-admin:DescribePermissionSet",
                "sso-admin:GetInlinePolicyForPermissionSet",
                "sso-admin:ListManagedPoliciesInPermissionSet",
                "sso-admin:ListAccountAssignments",
                "sso-admin:ListAccountsForProvisionedPermissionSet",
                "organizations:ListAccounts",
                "iam:GetPolicy",
                "iam:GetPolicyVersion"
            ],
            "Resource": "*"
        }
    ]
}
```

## Installation

1. Clone Repository

```bash
git clone https://github.com/FreddyFFM/iam_idc_matrix_generator.git
cd iam_idc_matrix
```

2. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install required packages:

```bash
pip install -r requirements.txt
```

## Usage
Run the main script:

```bash
python main.py
```

Now you can open the pivot result in your browser, the link is written to the log output as follows:

```
INFO Pivot table generation completed successfully
INFO - Open the Pivot view via pivot/permissions_pivot_20241128_152034.html
```

## Contributing

- Fork the repository
- Create a feature branch
- Commit your changes
- Push to the branch
- Create a Pull Request

## License
MIT license

## Authors
FreddyFFM

## Acknowledgments

AWS IAM Identity Center documentation
Python community
Amazon Q Developer
