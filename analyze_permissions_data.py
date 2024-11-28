from datetime import datetime
import pandas as pd
import json
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_permission_sets_data(filename=None):
    """
    Load permission sets data from JSON file.
    If filename is not provided, loads the most recent file.
    """
    try:       
        # Load the JSON data
        df = pd.read_json(filename)
        
        # Parse the JSON strings in policy columns
        if 'InlinePolicy' in df.columns:
            df['InlinePolicy'] = df['InlinePolicy'].apply(
                lambda x: json.loads(x) if pd.notnull(x) and x else None
            )
        
        if 'ManagedPolicies' in df.columns:
            df['ManagedPolicies'] = df['ManagedPolicies'].apply(
                lambda x: json.loads(x) if pd.notnull(x) and x else None
            )
            
        if 'ManagedPolicyContents' in df.columns:
            df['ManagedPolicyContents'] = df['ManagedPolicyContents'].apply(
                lambda x: json.loads(x) if pd.notnull(x) and x else None
            )
        
        # Log summary statistics
        logging.info(f"\nLoaded data summary from {filename}:")
        logging.info(f"Total Permission Sets: {df['PermissionSetName'].nunique()}")
        logging.info(f"Total Accounts: {df['AccountId'].nunique()}")
        logging.info(f"Total Assignments: {len(df)}")
        
        return df
        
    except Exception as e:
        logging.error(f"Error loading permission sets data: {str(e)}")
        raise

def analyze_inline_policy(policy_json):
    """Analyze an inline policy for specific patterns or permissions."""
    if not isinstance(policy_json, dict):
        return {
            'Statements': []
        }
        
    analysis = {
        'Statements': []
    }
    
    # Process the policy
    statements = policy_json.get('Statement', [])
    if isinstance(statements, dict):
        statements = [statements]
    elif not isinstance(statements, list):
        return analysis
        
    for statement in statements:
        if not isinstance(statement, dict):
            continue
            
        statement_analysis = {
            'Effect': statement.get('Effect', ''),
            'Actions': extract_actions(statement),
            'Resources': extract_resources(statement),
            'Conditions': extract_conditions(statement)
        }
        analysis['Statements'].append(statement_analysis)
    
    return analysis

def extract_actions(statement):
    """Extract actions from a policy statement."""
    actions = statement.get('Action', [])
    if isinstance(actions, str):
        actions = [actions]
    return sorted(actions) if actions else []

def extract_resources(statement):
    """Extract resources from a policy statement."""
    resources = statement.get('Resource', [])
    if isinstance(resources, str):
        resources = [resources]
    return sorted(resources) if resources else []

def extract_conditions(statement):
    """Extract conditions from a policy statement."""
    conditions = statement.get('Condition', {})
    formatted_conditions = []
    
    for operator, condition_map in conditions.items():
        for key, values in condition_map.items():
            if isinstance(values, str):
                values = [values]
            for value in values:
                formatted_conditions.append(f"{operator}:{key}:{value}")
                
    return sorted(formatted_conditions)

def expand_policy_elements(row):
    """
    Extract and expand policy elements for a single row.
    Returns a list of dictionaries, one for each action in each statement.
    """
    expanded_rows = []
    # Only drop the policy-related columns that exist in the input
    columns_to_drop = ['InlinePolicy', 'ManagedPolicies', 'ManagedPolicyContents']
    base_row = row.drop([col for col in columns_to_drop if col in row.index]).to_dict()
    
    # Process inline policy if it exists
    if isinstance(row['InlinePolicy'], dict):
        try:
            inline_analysis = analyze_inline_policy(row['InlinePolicy'])
            for statement in inline_analysis['Statements']:
                for action in statement['Actions']:
                    expanded_row = base_row.copy()
                    expanded_row.update({
                        'Effect': statement['Effect'],
                        'Action': action,
                        'Resources': statement['Resources'],
                        'Conditions': statement['Conditions'],
                        'PolicyType': 'Inline',
                        'PolicyName': 'InlinePolicy',
                        'PolicyArn': None
                    })
                    expanded_rows.append(expanded_row)
        except Exception as e:
            logging.warning(f"Error processing inline policy for {row['PermissionSetName']}: {str(e)}")
    
    # Process managed policies if they exist
    managed_policies = row['ManagedPolicyContents']
    if isinstance(managed_policies, list):
        for policy in managed_policies:
            try:
                if not isinstance(policy, dict):
                    continue
                    
                policy_content = policy.get('Content')
                if not isinstance(policy_content, dict):
                    logging.warning(f"Invalid policy content format for {policy.get('Name', 'Unknown')}")
                    continue
                
                managed_analysis = analyze_inline_policy(policy_content)
                if not managed_analysis or not managed_analysis.get('Statements'):
                    logging.warning(f"No valid statements found in policy {policy.get('Name', 'Unknown')}")
                    continue
                
                for statement in managed_analysis['Statements']:
                    for action in statement['Actions']:
                        expanded_row = base_row.copy()
                        expanded_row.update({
                            'Effect': statement['Effect'],
                            'Action': action,
                            'Resources': statement['Resources'],
                            'Conditions': statement['Conditions'],
                            'PolicyType': 'Managed',
                            'PolicyName': policy.get('Name', 'Unknown'),
                            'PolicyArn': policy.get('Arn')
                        })
                        expanded_rows.append(expanded_row)
            except Exception as e:
                logging.warning(f"Error processing managed policy {policy.get('Name', 'Unknown')}: {str(e)}")
    
    return expanded_rows

def split_action(action):
    """Split an AWS action into service and specific action."""
    try:
        if ':' in action:
            service, specific_action = action.split(':', 1)
            return {'service': service, 'specific_action': specific_action}
        return {'service': action, 'specific_action': '*'}
    except Exception:
        return {'service': action, 'specific_action': '*'}

def analyze_permissions(filename=None):
    """Load and analyze permission sets data with detailed policy analysis."""
    try:
        if filename is None:
            # Find the most recent file
            data_dir = Path('./data')
            json_files = list(data_dir.glob('raw_permission_sets_*.json'))

            if not json_files:
                raise FileNotFoundError("No data files found")

            latest_file = max(json_files, key=lambda x: x.stat().st_mtime)
            filename = latest_file        

        # Load the data from specified file
        df = load_permission_sets_data(filename=filename)
        
        # Apply analysis and expand rows
        logging.info("Expanding policy elements...")
        expanded_rows = []
        for _, row in df.iterrows():
            expanded_rows.extend(expand_policy_elements(row))

        # Create new DataFrame with expanded rows
        expanded_df = pd.DataFrame(expanded_rows)

        # Split the Action column into Service and SpecificAction
        action_split = expanded_df['Action'].apply(split_action)
        expanded_df['Service'] = action_split.apply(lambda x: x['service'])
        expanded_df['SpecificAction'] = action_split.apply(lambda x: x['specific_action'])

        # Generate timestamp for output
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_base = Path('./data') / f'analyzed_permission_sets_expanded_{timestamp}'
        
        # Save as JSON
        json_filename = f"{output_base}.json"
        expanded_df.to_json(json_filename, orient='records', indent=2)
        logging.info(f"Saved analyzed data to: {json_filename}")

        # Prepare DataFrame for CSV
        csv_df = expanded_df.copy()
        # Convert lists to strings for CSV compatibility
        csv_df['Resources'] = csv_df['Resources'].apply(lambda x: '|'.join(x) if isinstance(x, list) else '')
        csv_df['Conditions'] = csv_df['Conditions'].apply(lambda x: '|'.join(x) if isinstance(x, list) else '')
        
        # Save as CSV
        csv_filename = f"{output_base}.csv"
        csv_df.to_csv(csv_filename, index=False)
        logging.info(f"Saved CSV data to: {csv_filename}")

        # Print analysis summary
        logging.info("\nAnalysis Summary:")
        logging.info(f"Total Permission Sets: {expanded_df['PermissionSetName'].nunique()}")
        logging.info(f"Total Unique Services: {expanded_df['Service'].nunique()}")
        logging.info(f"Total Unique Actions: {expanded_df['SpecificAction'].nunique()}")
        logging.info(f"Total Rows (One per Action): {len(expanded_df)}")

        # Policy type distribution
        policy_type_counts = expanded_df['PolicyType'].value_counts()
        logging.info("\nPolicy Type Distribution:")
        for policy_type, count in policy_type_counts.items():
            logging.info(f"{policy_type}: {count} actions")

        # Service distribution (top 10)
        service_counts = expanded_df['Service'].value_counts().head(10)
        logging.info("\nTop 10 Services:")
        for service, count in service_counts.items():
            logging.info(f"{service}: {count} actions")

        # Count unique elements
        unique_resources = set()
        unique_conditions = set()

        for resources in expanded_df['Resources'].dropna():
            unique_resources.update(resources)
        for conditions in expanded_df['Conditions'].dropna():
            unique_conditions.update(conditions)

        logging.info(f"\nTotal Unique Resources: {len(unique_resources)}")
        logging.info(f"Total Unique Conditions: {len(unique_conditions)}")

        # Sample output for verification
        sample_size = min(5, len(expanded_df))
        logging.info(f"\nSample of {sample_size} rows:")
        sample_df = expanded_df.sample(sample_size)

        for _, row in sample_df.iterrows():
            logging.info(f"\nPermission Set: {row['PermissionSetName']}")
            logging.info(f"Policy Type: {row['PolicyType']}")
            logging.info(f"Policy Name: {row['PolicyName']}")
            if row['PolicyArn']:
                logging.info(f"Policy ARN: {row['PolicyArn']}")
            logging.info(f"Effect: {row['Effect']}")
            logging.info(f"Service: {row['Service']}")
            logging.info(f"Action: {row['SpecificAction']}")
            logging.info(f"Resources Count: {len(row['Resources']) if isinstance(row['Resources'], list) else 0}")
            logging.info(f"Conditions Count: {len(row['Conditions']) if isinstance(row['Conditions'], list) else 0}")
            if isinstance(row['Resources'], list):
                logging.info("Resources: " + ", ".join(row['Resources']))
            if isinstance(row['Conditions'], list) and row['Conditions']:
                logging.info("Conditions: " + ", ".join(row['Conditions']))

        return json_filename

    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        raise

