import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from pivottablejs import pivot_ui

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_latest_data(filename=None):
    """
    Load the most recent data file from json.
    """
    try:
        if not filename:
            
            # Get all data files
            data_dir = './data'
            data_path = Path(data_dir)
            json_files = list(data_path.glob('analyzed_permission_sets_expanded_*.json'))
            
            # Find the most recent file
            all_files = json_files
            if not all_files:
                raise FileNotFoundError("No data files found")
                
            latest_file = max(all_files, key=lambda x: x.stat().st_mtime)
            logging.info(f"Loading data from: {latest_file}")
            filename = latest_file
        
        # Load data into dataframe
        df = pd.read_json(filename)
                
        return df
        
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        raise

def create_pivot_view(df, output_path='./data/pivot_view.html'):
    """
    Create an interactive pivot table view of the permissions data.
    """
    try:
        # Create a copy of the DataFrame to avoid modifying the original
        pivot_df = df.copy()
        
        # Convert list columns to string representation for pivot table
        if 'Resources' in pivot_df.columns:
            pivot_df['Resources'] = pivot_df['Resources'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
        if 'Conditions' in pivot_df.columns:
            pivot_df['Conditions'] = pivot_df['Conditions'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

        # Create the pivot table UI with default configuration
        pivot_ui(
            pivot_df,
            outfile_path=output_path,
            width="100%",  # Full width
            height="800px",  # Reasonable height
            rows=['AccountName', 'PermissionSetName'],  # Default row grouping
            cols=['Service'],  # Default column grouping
            vals=['Action'],  # Default value to aggregate
            aggregatorName='Count',  # Default aggregation method
            rendererName='Table',  # Default visualization
            unusedAttrsVertical=False  # Horizontal unused attributes
        )
        
        logging.info(f"Created interactive pivot table at: {output_path}")
        
        # Print summary statistics
        logging.info("\nData Summary:")
        logging.info(f"Total Accounts: {pivot_df['AccountId'].nunique()}")
        logging.info(f"Total Permission Sets: {pivot_df['PermissionSetName'].nunique()}")
        logging.info(f"Total Services: {pivot_df['Service'].nunique()}")
        logging.info(f"Total Actions: {len(pivot_df)}")
        
        # Account distribution
        logging.info("\nActions per Account:")
        account_summary = pivot_df.groupby(['AccountName', 'AccountId'])['Action'].count().sort_values(ascending=False)
        for (account_name, account_id), count in account_summary.items():
            logging.info(f"{account_name} ({account_id}): {count} actions")
        
        # Service distribution
        logging.info("\nTop 10 Services by Action Count:")
        service_summary = pivot_df['Service'].value_counts().head(10)
        for service, count in service_summary.items():
            logging.info(f"{service}: {count} actions")
        
        # Permission Set distribution
        logging.info("\nTop 10 Permission Sets by Action Count:")
        ps_summary = pivot_df['PermissionSetName'].value_counts().head(10)
        for ps, count in ps_summary.items():
            logging.info(f"{ps}: {count} actions")
        
    except Exception as e:
        logging.error(f"Error creating pivot table: {str(e)}")
        raise

def generate_pivot(filename=None):
    """Main function to generate pivot views from existing data."""
    try:
        # Load the most recent data file
        df = load_latest_data(filename)        
        
        # Generate timestamp for output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create output path
        data_dir = Path('./pivot')
        data_dir.mkdir(exist_ok=True)

        output_path = Path('./pivot') / f'permissions_pivot_{timestamp}.html'
        
        # Create pivot view
        create_pivot_view(df, output_path)
        
        logging.info("Pivot table generation completed successfully")
        
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        raise

