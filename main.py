import pandas as pd
import os
from province_splitter import split_by_province


def split_csv_by_utm_zone(input_file, output_dir, utm_zone_column="utm_zone"):
    """
    Splits a CSV file into multiple files based on the unique UTM zones.
    then runs province splitter on each file (each UTM zone is separated into provinces)
    Only includes data points with sample_date or meas_date from 2016 onwards.

    Parameters:
        input_file (str): Path to the input CSV file.
        output_dir (str): Directory where output files will be saved.
        utm_zone_column (str): Name of the column containing UTM zone identifiers.
    """
    # Get the parent folder name from input file path
    parent_folder = os.path.basename(os.path.dirname(input_file))
    
    # Create output subdirectory with parent folder name
    output_subdir = os.path.join(output_dir, parent_folder)
    os.makedirs(output_subdir, exist_ok=True)

    # Read the input CSV file
    try:
        data = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Check if the UTM zone column exists
    if utm_zone_column not in data.columns:
        print(f"Error: UTM zone column '{utm_zone_column}' not found in the input file.")
        return

    # Convert date columns to datetime if they exist
    date_columns = []
    if 'sample_date' in data.columns:
        # Handle sample_date format: '2005-11-10 0:00'
        data['sample_date'] = pd.to_datetime(data['sample_date'], errors='coerce')
        date_columns.append('sample_date')
    if 'meas_date' in data.columns:
        # Handle meas_date format: '2004-SEP-11'
        data['meas_date'] = pd.to_datetime(data['meas_date'], format='%Y-%b-%d', errors='coerce')
        date_columns.append('meas_date')

    if not date_columns:
        print("Warning: Neither 'sample_date' nor 'meas_date' columns found in the input file.")
        return

    # Filter data for dates from January 1, 2016 onwards
    cutoff_date = pd.to_datetime('2016-01-01')
    
    # Remove rows where both dates are NaT (Not a Time) due to parsing errors
    if len(date_columns) == 2:
        data = data.dropna(subset=date_columns, how='all')
    else:
        data = data.dropna(subset=date_columns)

    # Print the date range before filtering
    print("\nDate ranges before filtering:")
    for col in date_columns:
        print(f"{col}: {data[col].min()} to {data[col].max()}")

    # Filter based on available date columns - keep only data where ALL dates are from 2016 onwards
    initial_count = len(data)
    if len(date_columns) == 1:
        data = data[data[date_columns[0]] >= cutoff_date]
    else:
        # Changed OR (|) to AND (&) - ALL dates must be from 2016 or later
        data = data[(data['sample_date'] >= cutoff_date) & (data['meas_date'] >= cutoff_date)]

    # Print the date range after filtering
    print("\nDate ranges after filtering:")
    for col in date_columns:
        if not data.empty:
            print(f"{col}: {data[col].min()} to {data[col].max()}")
        else:
            print(f"{col}: No data after filtering")

    print(f"\nRecords before date filtering: {initial_count}")
    print(f"Records after date filtering: {len(data)}")

    if len(data) == 0:
        print("\nWarning: No data points found where all dates are from 2016 onwards!")
        return

    # Group data by UTM zone and save each group to a separate CSV file
    unique_zones = data[utm_zone_column].unique()
    for zone in unique_zones:
        zone_data = data[data[utm_zone_column] == zone]
        output_file = os.path.join(output_subdir, f'utm_zone_{zone}.csv')
        zone_data.to_csv(output_file, index=False)
        print(f"Saved UTM zone {zone} data to {output_file}")
        split_by_province(output_file, output_dir)
    
if __name__ == "__main__":
    input_csv = "C:/Users/saqla/OneDrive/Desktop/NRC_Data/All_data/Canada_GP_exact_coords_non-private/all_gp_trees_mixed/all_gp_trees_mixed/all_gp_site_info.csv"  # Replace with your input CSV file path
    output_directory = "C:/Users/saqla/OneDrive/Desktop/NRC_Data/Preprocessing"  # Replace with your desired output directory
    split_csv_by_utm_zone(input_csv, output_directory)
    # we get the files of each province with datapoints divided into separate files based on their UTM zones