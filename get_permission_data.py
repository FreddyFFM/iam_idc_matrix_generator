import boto3
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_instance_arn():
    """Get the SSO instance ARN and identity store ID."""
    sso_admin = boto3.client('sso-admin')
    
    try:
        instances = sso_admin.list_instances()
        if not instances['Instances']:
            raise ValueError("No SSO instance found")
            
        instance = instances['Instances'][0]
        return instance['InstanceArn'], instance['IdentityStoreId']
        
    except Exception as e:
        logging.error(f"Error getting SSO instance: {str(e)}")
        raise

def get_permission_sets(instance_arn):
    """Get all permission sets."""
    sso_admin = boto3.client('sso-admin')
    permission_sets = []
    
    try:
        paginator = sso_admin.get_paginator('list_permission_sets')
        for page in paginator.paginate(InstanceArn=instance_arn):
            permission_sets.extend(page['PermissionSets'])
            
        return permission_sets
        
    except Exception as e:
        logging.error(f"Error getting permission sets: {str(e)}")
        raise

def get_managed_policy_content(policy_arn):
    """Get the content of a managed policy."""
    iam = boto3.client('iam')
    try:
        # Get the default version ID of the policy
        policy = iam.get_policy(PolicyArn=policy_arn)
        default_version_id = policy['Policy']['DefaultVersionId']
        
        # Get the policy content
        policy_version = iam.get_policy_version(
            PolicyArn=policy_arn,
            VersionId=default_version_id
        )
        return policy_version['PolicyVersion']['Document']
    except Exception as e:
        logging.warning(f"Error getting managed policy content for {policy_arn}: {str(e)}")
        return None

def get_permission_set_details(instance_arn, permission_set_arn):
    """Get details for a specific permission set including policies."""
    sso_admin = boto3.client('sso-admin')
    
    try:
        # Get permission set details
        details = sso_admin.describe_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set_arn
        )['PermissionSet']
        
        # Get inline policy
        try:
            inline_policy_str = sso_admin.get_inline_policy_for_permission_set(
                InstanceArn=instance_arn,
                PermissionSetArn=permission_set_arn
            )['InlinePolicy']
            
            # Ensure the inline policy is a valid JSON string
            if inline_policy_str and isinstance(inline_policy_str, str):
                try:
                    inline_policy = json.loads(inline_policy_str)
                except json.JSONDecodeError as e:
                    logging.warning(f"Invalid JSON in inline policy for {permission_set_arn}: {e}")
                    inline_policy = None
            else:
                inline_policy = None

        except sso_admin.exceptions.ResourceNotFoundException:
            inline_policy = None
        
        # Get managed policies and their contents
        managed_policies = []
        try:
            paginator = sso_admin.get_paginator('list_managed_policies_in_permission_set')
            for page in paginator.paginate(
                InstanceArn=instance_arn,
                PermissionSetArn=permission_set_arn
            ):
                for policy in page['AttachedManagedPolicies']:
                    policy_content = get_managed_policy_content(policy['Arn'])
                    managed_policies.append({
                        'Name': policy['Name'],
                        'Arn': policy['Arn'],
                        'Content': policy_content
                    })
        except Exception as e:
            logging.warning(f"Error getting managed policies: {str(e)}")
        
        return {
            'Name': details['Name'],
            'PermissionSetArn': permission_set_arn,
            'Description': details.get('Description', ''),
            'InlinePolicy': inline_policy,
            'ManagedPolicies': managed_policies
        }
        
    except Exception as e:
        logging.error(f"Error getting permission set details: {str(e)}")
        raise

def get_account_assignments(instance_arn, permission_set_arn):
    """Get account assignments for a permission set."""
    sso_admin = boto3.client('sso-admin')
    assignments = []
    
    try:
        assignments = []
        paginator = sso_admin.get_paginator('list_account_assignments')
        
        accounts_response = sso_admin.list_accounts_for_provisioned_permission_set(
            InstanceArn=instance_arn,
            PermissionSetArn=permission_set_arn
        )
            
        for account in accounts_response['AccountIds']:
            for page in paginator.paginate(
                InstanceArn=instance_arn,
                AccountId=account,
                PermissionSetArn=permission_set_arn
            ):
                for assignment in page['AccountAssignments']:
                    assignments.append({
                        'AccountId': account,
                        'PrincipalType': assignment['PrincipalType'],
                        'PrincipalId': assignment['PrincipalId']
                    })
        
        return assignments
        
    except Exception as e:
        logging.error(f"Error getting account assignments: {str(e)}")
        raise

def get_account_names():
    """Get a dictionary of account IDs to account names using Organizations API."""
    try:
        org_client = boto3.client('organizations')
        account_names = {}
        
        paginator = org_client.get_paginator('list_accounts')
        for page in paginator.paginate():
            for account in page['Accounts']:
                account_names[account['Id']] = account['Name']
                
        logging.info(f"Retrieved {len(account_names)} account names from Organizations")
        return account_names
    except Exception as e:
        logging.warning(f"Error getting account names from Organizations: {e}")
        return {}

def create_permission_sets_dataframe():
    """Create a DataFrame with permission sets and their assignments."""
    try:
        instance_arn, identity_store_id = get_instance_arn()
        logging.info("Got SSO instance details")

        # Get account names
        account_names = get_account_names()
        logging.info("Retrieved account names from Organizations")
        
        # Get all permission sets
        permission_sets = get_permission_sets(instance_arn)
        logging.info(f"Found {len(permission_sets)} permission sets")
        
        # Collect data for DataFrame
        data = []
        for permission_set_arn in permission_sets:
            try:
                # Get permission set details
                details = get_permission_set_details(instance_arn, permission_set_arn)
                
                # Get assignments
                assignments = get_account_assignments(instance_arn, permission_set_arn)
                
                # Add to data list
                for assignment in assignments:
                    data.append({
                        'PermissionSetName': details['Name'],
                        'PermissionSetArn': permission_set_arn,
                        'Description': details['Description'],
                        'AccountId': assignment['AccountId'],
                        'AccountName': account_names.get(assignment['AccountId'], 'Unknown'),                        
                        'InlinePolicy': json.dumps(details['InlinePolicy']) if details['InlinePolicy'] else None,
                        'ManagedPolicies': json.dumps([{
                            'Name': p['Name'], 
                            'Arn': p['Arn']
                        } for p in details['ManagedPolicies']]) if details['ManagedPolicies'] else None,
                        'ManagedPolicyContents': json.dumps([{
                            'Name': p['Name'],
                            'Arn': p['Arn'],
                            'Content': p['Content']
                        } for p in details['ManagedPolicies']]) if details['ManagedPolicies'] else None
                    })
                    
            except Exception as e:
                logging.warning(f"Error processing permission set {permission_set_arn}: {str(e)}")
                continue
        
        # Create DataFrame
        df = pd.DataFrame(data)
        
        return df
        
    except Exception as e:
        logging.error(f"Error creating DataFrame: {str(e)}")
        raise

def store_permission_sets_data():
    """
    Store permission sets data with assignments and policies to files.
    Returns the DataFrame for potential further processing.
    """
    try:
        # Create DataFrame
        df = create_permission_sets_dataframe()
        
        # Generate timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create data directory if it doesn't exist
        data_dir = Path('./data')
        data_dir.mkdir(exist_ok=True)
        
        # Create filenames with path
        csv_filename = data_dir / f'raw_permission_sets_{timestamp}.csv'
        json_filename = data_dir / f'raw_permission_sets_{timestamp}.json'
        
        # Save to CSV (with raw JSON strings preserved)
        df.to_csv(csv_filename, index=False)
        logging.info(f"Saved CSV file: {csv_filename}")
        
        # Save to JSON (preserves the data types better than CSV)
        df.to_json(json_filename, orient='records', indent=2)
        logging.info(f"Saved JSON file: {json_filename}")
        
        # Log summary statistics
        logging.info(f"Stored data summary:")
        logging.info(f"Total Permission Sets: {df['PermissionSetName'].nunique()}")
        logging.info(f"Total Accounts: {df['AccountId'].nunique()}")
        logging.info(f"Total Assignments: {len(df)}")
        account_summary = df.groupby(['AccountId', 'AccountName']).size().sort_values(ascending=False)
        for (account_id, account_name), count in account_summary.items():
            logging.info(f"{account_name} ({account_id}): {count} permission sets")
        
        return json_filename
        
    except Exception as e:
        logging.error(f"Error storing permission sets data: {str(e)}")
        raise

