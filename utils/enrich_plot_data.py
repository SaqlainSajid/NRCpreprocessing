import sqlite3
import pandas as pd
import os
from datetime import datetime

def get_table_data(conn, table_name, nfi_plots, chunk_size=1000):
    """
    Get data from a specific table for given nfi_plots in chunks
    """
    chunks = [nfi_plots[i:i + chunk_size] for i in range(0, len(nfi_plots), chunk_size)]
    results = []
    
    for chunk in chunks:
        placeholders = ','.join(['?' for _ in chunk])
        query = f"""
        SELECT *
        FROM {table_name}
        WHERE nfi_plot IN ({placeholders})
        """
        chunk_df = pd.read_sql_query(query, conn, params=chunk)
        results.append(chunk_df)
    
    return pd.concat(results) if results else pd.DataFrame()

def enrich_plot_data(input_csv_path, chunk_size=1000):
    """
    Enrich plot data from an input CSV file with additional attributes from various database tables.
    Process one table at a time using chunking for better performance.
    """
    # Create output directory if it doesn't exist
    output_dir = 'enriched_data'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Generate output filename
    base_name = os.path.basename(input_csv_path)
    output_path = os.path.join(output_dir, f"enriched_{base_name}")
    
    print(f"Starting data enrichment process for {base_name}...")
    
    try:
        # Read the input CSV file
        df_input = pd.read_csv(input_csv_path)
        total_plots = len(df_input)
        print(f"Found {total_plots} plots in input file")
        
        # Get list of nfi_plots
        nfi_plots = df_input['nfi_plot'].unique().tolist()
        
        # Connect to the database
        conn = sqlite3.connect('forest_inventory.db')
        
        # Define tables to process and their key columns
        tables = {
            'all_pp_photo_plot': ['utm_e', 'utm_n', 'utm_zone', 'nomplot_size'],
            'all_pp_landcover': ['land_base', 'land_cover', 'land_pos', 'veg_type', 'density_cl', 'stand_stru', 'soil_moist', 'devel_stage'],
            'all_pp_poly_summ': ['vol', 'vol_merch', 'closure', 'site_age', 'site_height', 'site_index', 'biomass_total_live', 'biomass_total_dead'],
            'all_pp_std_layer_disturbance': ['dist_agent', 'dist_yr', 'dist_perct', 'mort_perct', 'agent_type'],
            'all_pp_std_layer_tree_sp': ['species_num', 'genus', 'species', 'percent', 'height', 'age'],
            'all_pp_landuse': ['landuse1', 'landuse2'],
            'all_pp_ownership': ['ownership'],
            'all_pp_protect_status': ['status']
        }
        
        # Process each table
        enriched_df = df_input.copy()
        
        for table_name, columns in tables.items():
            print(f"\nProcessing {table_name}...")
            
            # Get data from current table
            table_df = get_table_data(conn, table_name, nfi_plots, chunk_size)
            
            if not table_df.empty:
                # Add suffix to avoid column name conflicts
                suffix = f"_{table_name.split('_')[-1]}"
                table_df = table_df.add_suffix(suffix)
                table_df.rename(columns={f'nfi_plot{suffix}': 'nfi_plot'}, inplace=True)
                
                # Merge with existing data
                enriched_df = pd.merge(
                    enriched_df,
                    table_df,
                    on='nfi_plot',
                    how='left'
                )
                
                print(f"Added {len(columns)} columns from {table_name}")
            else:
                print(f"No matching data found in {table_name}")
        
        # Save enriched data
        print(f"\nSaving enriched data to {output_path}")
        enriched_df.to_csv(output_path, index=False)
        
        # Print summary
        print("\nEnrichment complete!")
        print(f"Original columns: {len(df_input.columns)}")
        print(f"Enriched columns: {len(enriched_df.columns)}")
        print(f"New attributes added: {len(enriched_df.columns) - len(df_input.columns)}")
        
        # Print sample of columns added
        new_columns = set(enriched_df.columns) - set(df_input.columns)
        print("\nSample of new columns added:")
        print(", ".join(sorted(list(new_columns))[:10]))
        
    except Exception as e:
        print(f"Error during enrichment: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Get input file path from user
    input_file = r"C:/Users/saqla/OneDrive/Desktop/NRC_Data/Preprocessing/plots_from_2016/AB/singlepoint.csv"
    
    # Verify file exists
    if os.path.exists(input_file):
        enrich_plot_data(input_file)
    else:
        print("Error: File not found!")
