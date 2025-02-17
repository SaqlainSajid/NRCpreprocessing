import sqlite3
import pandas as pd
import os
from pathlib import Path

def get_table_data(conn, table_name, plot_ids, columns, has_info_date=True):
    """Get data from a specific table for given plot IDs"""
    # Split plot_ids into chunks to handle large datasets
    chunk_size = 1000
    plot_id_chunks = [plot_ids[i:i + chunk_size] for i in range(0, len(plot_ids), chunk_size)]
    
    all_data = []
    for chunk in plot_id_chunks:
        if has_info_date:
            columns_str = ','.join(['nfi_plot', 'info_date'] + columns)
            query = f"""
            WITH RankedData AS (
                SELECT 
                    {columns_str},
                    ROW_NUMBER() OVER (PARTITION BY nfi_plot ORDER BY info_date DESC) as rn
                FROM {table_name}
                WHERE nfi_plot IN ({','.join(['?']*len(chunk))})
            )
            SELECT {columns_str}
            FROM RankedData
            WHERE rn = 1
            """
        else:
            # For tables without info_date, just get the data directly
            columns_str = ','.join(['nfi_plot'] + columns)
            query = f"""
            SELECT {columns_str}
            FROM {table_name}
            WHERE nfi_plot IN ({','.join(['?']*len(chunk))})
            """
        chunk_data = pd.read_sql_query(query, conn, params=chunk)
        all_data.append(chunk_data)
    
    return pd.concat(all_data) if all_data else pd.DataFrame(columns=['nfi_plot'] + columns)

def inspect_table(conn, table_name, sample_size=5):
    """Inspect a table's structure and sample data"""
    print(f"\n=== Inspecting table: {table_name} ===")
    
    # Get column information
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    print("\nColumns:")
    for col in columns:
        print(f"- {col[1]} ({col[2]})")
    
    # Get sample data
    query = f"SELECT * FROM {table_name} LIMIT {sample_size}"
    sample_df = pd.read_sql_query(query, conn)
    print(f"\nSample data ({sample_size} rows):")
    print(sample_df)
    
    # Get value counts for categorical columns
    categorical_cols = sample_df.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        value_counts = pd.read_sql_query(
            f"SELECT {col}, COUNT(*) as count FROM {table_name} GROUP BY {col} LIMIT 5",
            conn
        )
        print(f"\nTop values for {col}:")
        print(value_counts)

def get_mortality_data(input_csv_path, output_directory):
    """
    Process an input CSV file containing NFI plot data and join it with mortality-related attributes
    using Python-based matching. Takes only the most recent record for each plot.
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)
        
        # Read input CSV file
        print("Reading input CSV file...")
        input_df = pd.read_csv(input_csv_path)
        print(f"\nInput file columns: {input_df.columns.tolist()}")
        
        # Keep only the most recent record for each plot and polygon
        input_df['info_date'] = pd.to_datetime(input_df['info_date'])
        input_df = input_df.sort_values('info_date', ascending=False)
        input_df = input_df.drop_duplicates(subset=['nfi_plot', 'poly_id'], keep='first')
        
        plot_ids = input_df['nfi_plot'].unique().tolist()
        print(f"\nNumber of unique plot IDs: {len(plot_ids)}")
        
        if len(plot_ids) == 0:
            raise ValueError("No NFI plot IDs found in the input file")
            
        # Connect to the database
        conn = sqlite3.connect('forest_inventory.db')
        
        # First, inspect the land cover table structure
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(all_pp_landcover)")
        lc_columns = cursor.fetchall()
        print("\nLand cover table columns:")
        available_columns = []
        for col in lc_columns:
            col_name = col[1]
            print(f"- {col_name} ({col[2]})")
            available_columns.append(col_name)
            
        # Also get a sample row to verify the actual data
        cursor.execute("""
            SELECT * FROM all_pp_landcover LIMIT 1
        """)
        sample_row = cursor.fetchone()
        if sample_row:
            col_names = [description[0] for description in cursor.description]
            print("\nActual column names in data:")
            for i, name in enumerate(col_names):
                print(f"{i}: {name}")
        
        # Get data from each table separately
        print("\nGetting disturbance data...")
        dist_cols = ['dist_agent', 'dist_yr', 'mort_perct', 'mort_basis', 'agent_type']
        dist_df = get_table_data(conn, 'all_pp_std_layer_disturbance', plot_ids, dist_cols, True)
        if not dist_df.empty:
            # Keep only the most recent record per plot
            dist_df = dist_df.sort_values('info_date', ascending=False)
            dist_df = dist_df.drop_duplicates(subset=['nfi_plot'], keep='first')
            dist_df.drop('info_date', axis=1, inplace=True)
        
        print("Getting tree species data...")
        tree_cols = ['genus', 'species', 'percent']
        tree_df = get_table_data(conn, 'all_pp_std_layer_tree_sp', plot_ids, tree_cols, True)
        if not tree_df.empty:
            # Filter for conifers and aggregate percentages
            conifer_genera = ['Picea', 'Pinus', 'Abies', 'Larix', 'Tsuga', 'Thuja', 'Pseudotsuga']
            tree_df = tree_df[tree_df['genus'].isin(conifer_genera)]
            # Sort by date and get most recent records
            tree_df = tree_df.sort_values('info_date', ascending=False)
            # Group by plot and aggregate
            tree_df = tree_df.groupby('nfi_plot').agg({
                'genus': lambda x: ','.join(sorted(set(x.iloc[0:1]))),  # Take genus from most recent record
                'species': lambda x: ','.join(sorted(set(x.iloc[0:1]))),  # Take species from most recent record
                'percent': 'first'  # Take percent from most recent record
            }).reset_index()
        
        print("Getting biomass data...")
        biomass_cols = ['biomass_total_dead', 'biomass_total_live']
        biomass_df = get_table_data(conn, 'all_pp_poly_summ', plot_ids, biomass_cols, False)
        if not biomass_df.empty:
            biomass_df = biomass_df.drop_duplicates(subset=['nfi_plot'], keep='first')
        
        print("Getting land cover data...")
        # Use the actual column name from inspection
        lc_cols = []
        vegetation_columns = ['veg_type', 'vegtype', 'vegetation_type', 'veg']  # Try different possible names
        for col in vegetation_columns:
            if col in available_columns:
                lc_cols.append(col)
                break
                
        if lc_cols:
            print(f"Using land cover columns: {lc_cols}")
            lc_df = get_table_data(conn, 'all_pp_landcover', plot_ids, lc_cols, True)
            if not lc_df.empty:
                # Keep only the most recent record per plot
                lc_df = lc_df.sort_values('info_date', ascending=False)
                lc_df = lc_df.drop_duplicates(subset=['nfi_plot'], keep='first')
                lc_df.drop('info_date', axis=1, inplace=True)
        else:
            print("Warning: Could not find vegetation type column in land cover table")
            print("Available columns:", available_columns)
            lc_df = pd.DataFrame(columns=['nfi_plot'])
        
        # Close database connection
        conn.close()
        
        # Start with the input dataframe
        print("Merging data...")
        result_df = input_df.copy()
        
        # Merge with logging to track missing data
        dfs_to_merge = [
            ('disturbance', dist_df),
            ('tree species', tree_df),
            ('biomass', biomass_df)
        ]
        
        if len(lc_df.columns) > 1:  # Only merge if we have data columns beyond nfi_plot
            dfs_to_merge.append(('land cover', lc_df))
        
        for name, df in dfs_to_merge:
            if df is not None and not df.empty:
                before_count = len(result_df)
                result_df = result_df.merge(df, on='nfi_plot', how='left')
                after_count = len(result_df)
                missing_count = result_df[df.columns[1:]].isna().any(axis=1).sum()
                print(f"\nMerging {name} data:")
                print(f"Records before merge: {before_count}")
                print(f"Records after merge: {after_count}")
                print(f"Records with missing {name} data: {missing_count}")
                
                # Verify no duplicate records
                if len(result_df) != len(result_df.drop_duplicates()):
                    print(f"Warning: Found duplicates after {name} merge")
                    result_df = result_df.drop_duplicates()
        
        # Generate output filename
        input_filename = Path(input_csv_path).stem
        output_path = os.path.join(output_directory, f"{input_filename}_mortality.csv")
        
        # Save the results
        result_df.to_csv(output_path, index=False)
        print(f"\nResults saved to: {output_path}")
        print(f"Total records: {len(result_df)}")
        
        # Print summary statistics
        print("\nSummary Statistics:")
        print(f"Total NFI Plots: {len(result_df)}")
        print(f"Plots with mortality data: {result_df['mort_perct'].notna().sum()}")
        print(f"Plots with conifer species: {result_df['genus'].notna().sum()}")
        
        # Print distribution of disturbance agents if present
        if not result_df['dist_agent'].isna().all():
            print("\nDisturbance Agent Distribution:")
            print(result_df['dist_agent'].value_counts().head())
            
        # Print distribution of conifer genera if present
        if not result_df['genus'].isna().all():
            print("\nConifer Genera Distribution:")
            print(result_df['genus'].value_counts().head())
            
    except Exception as e:
        print(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    # Hard-coded paths
    input_directory = "plots_before_2016"
    province = "AB"
    input_csv = os.path.join(input_directory, province, "all_zones.csv")
    output_directory = "mortality_data"
    
    get_mortality_data(input_csv, output_directory)
