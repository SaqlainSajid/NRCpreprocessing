import sqlite3
import pandas as pd
import os

def extract_table_data(conn, table_name, nfi_plot):
    """
    Extract all rows from a table for a specific nfi_plot
    """
    try:
        # Convert nfi_plot to integer
        nfi_plot = int(nfi_plot)
        
        # Use parameterized query
        query = f"SELECT * FROM {table_name} WHERE nfi_plot = ?"
        
        # Add extra debugging for disturbance table
        if table_name == 'all_pp_std_layer_disturbance':
            print(f"    Checking disturbance table for nfi_plot {nfi_plot}")
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM all_pp_std_layer_disturbance")
            total = cursor.fetchone()[0]
            print(f"    Total rows in disturbance table: {total}")
            cursor.execute("SELECT COUNT(*) FROM all_pp_std_layer_disturbance WHERE nfi_plot = ?", (nfi_plot,))
            matches = cursor.fetchone()[0]
            print(f"    Matching rows for nfi_plot {nfi_plot}: {matches}")
            
        df = pd.read_sql_query(query, conn, params=(nfi_plot,))
        
        print(f"    Found {len(df)} rows")
        return df
    except Exception as e:
        print(f"Error querying {table_name}: {str(e)}")
        return pd.DataFrame()

def process_nfi_plot(nfi_plot, tables, conn):
    """
    Process a single nfi_plot and extract data from all tables
    """
    try:
        # Convert nfi_plot to integer
        nfi_plot = int(nfi_plot)
        print(f"\nProcessing nfi_plot: {nfi_plot}")
        
        # Create directory for this nfi_plot
        plot_dir = os.path.join('data', f'nfi_plot_{nfi_plot}_data')
        os.makedirs(plot_dir, exist_ok=True)
        
        # Process each table
        for table in tables:
            print(f"\nExtracting data from {table}...")
            df = extract_table_data(conn, table, nfi_plot)
            
            if not df.empty:
                # Save to CSV
                output_file = os.path.join(plot_dir, f'{table}_nfi_plot_{nfi_plot}.csv')
                df.to_csv(output_file, index=False)
                print(f"    Saved to {output_file}")
            else:
                print(f"    No data found")
                
    except ValueError as e:
        print(f"Error: Invalid nfi_plot value {nfi_plot} - {str(e)}")

def extract_nfi_plot_data(input_csv_path):
    """
    Extract data for each unique nfi_plot from the input CSV file
    """
    # Create main data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Tables to process
    tables = [
        'all_pp_landcover',
        'all_pp_poly_summ',
        'all_pp_std_layer_disturbance',
        'all_pp_std_layer_header',
        'all_pp_std_layer_origin',
        'all_pp_std_layer_treatment',
        'all_pp_std_layer_tree_sp',
        'all_pp_landuse',
        'all_pp_ownership',
        'all_pp_protect_status'
    ]
    
    try:
        # Read input CSV and get unique nfi_plots
        print(f"Reading input file: {input_csv_path}")
        df_input = pd.read_csv(input_csv_path)
        
        # Ensure nfi_plot is read as integer
        df_input['nfi_plot'] = df_input['nfi_plot'].astype(int)
        unique_plots = df_input['nfi_plot'].unique()
        
        print(f"Found {len(unique_plots)} unique nfi_plots")
        
        # Connect to database
        conn = sqlite3.connect('forest_inventory.db')
        
        # Process each unique nfi_plot
        for plot_num, nfi_plot in enumerate(unique_plots, 1):
            print(f"\nProcessing plot {plot_num} of {len(unique_plots)}")
            process_nfi_plot(nfi_plot, tables, conn)
        
        print("\nExtraction complete!")
        print(f"Data has been saved in individual directories under the 'data' folder")
        
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    # Input file path
    input_file = r"C:/Users/saqla/OneDrive/Desktop/NRC_Data/Preprocessing/plots_from_2016/AB/singlepoint.csv"
    
    if os.path.exists(input_file):
        extract_nfi_plot_data(input_file)
    else:
        print("Error: Input file not found!")
